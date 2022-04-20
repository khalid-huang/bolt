// +build !windows,!plan9,!solaris

package bolt

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// flock acquires an advisory lock on a file descriptor.
func flock(db *DB, mode os.FileMode, exclusive bool, timeout time.Duration) error {
	var t time.Time
	for {
		// If we're beyond our timeout then return an error.
		// This can only occur after we've attempted a flock once.
		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return ErrTimeout
		}
		flag := syscall.LOCK_SH
		if exclusive {
			flag = syscall.LOCK_EX
		}

		// Otherwise attempt to obtain an exclusive lock.
		err := syscall.Flock(int(db.file.Fd()), flag|syscall.LOCK_NB)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK {
			return err
		}

		// Wait for a bit and try again.
		time.Sleep(50 * time.Millisecond)
	}
}

// funlock releases an advisory lock on a file descriptor.
func funlock(db *DB) error {
	return syscall.Flock(int(db.file.Fd()), syscall.LOCK_UN)
}

// mmap memory maps a DB's data file.
// 这里初始化数据库的时候，sz为32k，刚开始时，db文件大小为16k。这个时候映射32KB的文件会不会有问题？window平台和linux平台对此有不同的处理:
// 在针对windows平台的实现中，在进行mmap映射之前都会通过ftruncate系统调用将文件大小调整为待映射的大小，而在linux/unix平台的实现中是直接进行mmap调用的：
// mmap也是以页为单位进行映射的，如果文件大小不是页大小的整数倍，映射的最后一页肯定超过了文件结尾处，这个时候超过部分的内存会初始化为0，
// 对其的写操作不会写入文件。但如果映射的内存范围超过了文件大小，且超出范围大于4k，那对于超过文件所在最后一页地址空间的访问将引发异常。
// 比如我们这里文件实际大小是16K，但我们要映射32K到进程地址空间中，那对超过16K部分的内存访问将会引发异常。实际上，我们前面分析过，
// Boltdb通过mmap进行了只读映射，故不会存在通过内存映射写文件的问题，同时，对db.data(即映射的内存区域)的访问是通过pgid来访问的，
// 当前database文件里实际包含多少个page是记录在meta中的，每次通过db.data来读取一页时，boltdb均会作超限判断的，
// 所以不会存在对超过当前文件实际页数以外的区域访问的情况。正如我们在db.init()中看到的，此时meta中记录的pgid为4，即当前数据库文件总的page数为4，
// 故即使mmap映射长度为32KB，通过pgid索引也不会访问到16KB以外的地址空间。这个我们在后面的代码分析中会再次提及，这里可以暂时略过。
// 需要说明的是，当对数据库进行写操作时，如果要增加文件大小，针对linux/unix系统，boltdb也会通过ftruncate系统调用增加文件大小，
// 但是它并不是为了避免访问映射区域发生异常的问题，因为boltdb写文件不是通过mmap，而是直接通过fwrite写文件。
// 强调一下，boltdb对数据库的读操作是通过读mmap内存映射区完成的；而写操作是通过文件fseek及fwrite系统调用完成的。
func mmap(db *DB, sz int) error {
	// Map the data file to memory.
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return err
	}

	// Advise the kernel that the mmap is accessed randomly.
	if err := madvise(b, syscall.MADV_RANDOM); err != nil {
		return fmt.Errorf("madvise: %s", err)
	}

	// Save the original byte slice and convert to a byte array pointer.
	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

// munmap unmaps a DB's data file from memory.
func munmap(db *DB) error {
	// Ignore the unmap if we have no mapped data.
	if db.dataref == nil {
		return nil
	}

	// Unmap using the original byte slice.
	err := syscall.Munmap(db.dataref)
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return err
}

// NOTE: This function is copied from stdlib because it is not available on darwin.
func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
