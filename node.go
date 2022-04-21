package bolt

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

// node represents an in-memory, deserialized page.
type node struct {
	bucket     *Bucket // 节点所在桶的指针
	isLeaf     bool // 是否为叶子节点
	// 调整和维护B+树时需要的
	unbalanced bool // 是否需要进行合并，当节点有KV对被删除时，改值设为true
	spilled    bool // 是否需要进场拆分，指示当前node是否已经溢出(spill)，node溢出是指当node的size超过页大小，即一页无法存node中所有K/V对时，节点会分裂成多个node，以保证每个node的size小于页大小，分裂后产生新的node会增加父node的size，故父node也可能需要分裂；已经溢出过的node不需要再分裂;

	key        []byte // 锁包含第一个元素的key，boltdb的索引节点的指针和索引值数量是一样的
	pgid       pgid // 对应的页id

	parent     *node // 父节点指针
	children   nodes // 孩子节点
	inodes     inodes // 所存元素的元信息；对于分支节点是 key+pgid 数组(也就是索引信息)，对于叶子节点是 kv 数组
}

// root returns the top-level node this node is attached to.
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// minKeys returns the minimum number of inodes this node should have.
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

// size returns the size of the node after serialization.
func (n *node) size() int {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
	}
	return sz
}

// sizeLessThan returns true if the node is less than a given size.
// This is an optimization to avoid calculating a large node when we only need
// to know if it fits inside a certain page size.
func (n *node) sizeLessThan(v int) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
		if sz >= v {
			return false
		}
	}
	return true
}

// pageElementSize returns the size of each page element based on the type of node.
func (n *node) pageElementSize() int {
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

// childAt returns the child node at a given index.
func (n *node) childAt(index int) *node {
	if n.isLeaf {
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}
	return n.bucket.node(n.inodes[index].pgid, n)
}

// childIndex returns the index of a given child node.
func (n *node) childIndex(child *node) int {
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, child.key) != -1 })
	return index
}

// numChildren returns the number of children.
func (n *node) numChildren() int {
	return len(n.inodes)
}

// nextSibling returns the next node with the same parent.
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

// prevSibling returns the previous node with the same parent.
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

// 所有的数据新增都发生在叶子节点，如果新增数据后 B+ 树不平衡，之后会通过 node.spill 来进行拆分调整
// put inserts a key/value.
// 在插入的时候，oldKey等于newKey
// spill的时候，old不等于newKey
func (n *node) put(oldKey, newKey, value []byte, pgid pgid, flags uint32) {
	if pgid >= n.bucket.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", pgid, n.bucket.tx.meta.pgid))
	} else if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}

	// Find insertion index.
	// 二分查找找插入位置
	// 先查找oldKey以确定插入的位置。要么是正好找到oldKey，则插入位置就是oldKey的位置，实际上是替换；要么node中不含有oldKey，
	// 则插入位置就是第一个大于oldKey的前一个key的位置或者是节点结尾;
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, oldKey) != -1 })

	// 如果找到oldKey，则直接用newKey和value替代原来的oldKey和old value；
	// 如果未找到oldKey，则向node中添加一个inode，并将插入位置后的所有inode向后移一位，以实现插入新的inode;
	// Add capacity and shift nodes if we don't have an exact match and need to insert.
	exact := (len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey))
	if !exact {
		n.inodes = append(n.inodes, inode{})
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	inode := &n.inodes[index]
	inode.flags = flags
	inode.key = newKey
	inode.value = value
	inode.pgid = pgid
	_assert(len(inode.key) > 0, "put: zero-length inode key")
}

// del removes a key from the node.
func (n *node) del(key []byte) {
	// Find index of key.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, key) != -1 })

	// Exit if the key isn't found.
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].key, key) {
		return
	}

	// Delete inode from the node.
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)

	// Mark the node as needing rebalancing.
	n.unbalanced = true
}

// read initializes the node from a page.
func (n *node) read(p *page) {
	n.pgid = p.id
	n.isLeaf = ((p.flags & leafPageFlag) != 0)
	n.inodes = make(inodes, int(p.count))

	for i := 0; i < int(p.count); i++ {
		inode := &n.inodes[i]
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
		_assert(len(inode.key) > 0, "read: zero-length inode key")
	}

	// Save first key so we can find the node in the parent when we spill.
	if len(n.inodes) > 0 {
		n.key = n.inodes[0].key
		_assert(len(n.key) > 0, "read: zero-length node key")
	} else {
		n.key = nil
	}
}

// write writes the items onto one or more pages.
func (n *node) write(p *page) {
	// Initialize page.
	// 根据node是否是叶子节点确定page是leafPage还是branchPage
	if n.isLeaf {
		p.flags |= leafPageFlag
	} else {
		p.flags |= branchPageFlag
	}

	if len(n.inodes) >= 0xFFFF {
		panic(fmt.Sprintf("inode overflow: %d (pgid=%d)", len(n.inodes), p.id))
	}
	p.count = uint16(len(n.inodes))

	// Stop here if there are no items to write.
	if p.count == 0 {
		return
	}

	// Loop over each item and write it to the page.
	// 指向elements的结尾处，而且elements是从页正文开始存的
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[n.pageElementSize()*len(n.inodes):]
	// 通过for循环将所有kv记录顺序写入页
	// 通过上面的代码知道，页中存储K/V是以Key、Value交替连续存储的，所有的K/V对是在页中的elements结构结尾处存储，
	// 且每个element通过pos指向自己对应的K/V对，pos的值即是K/V对相对element起始位置的偏移；需要说明的是，
	// B+Tree中的内结点并不存真正的K/V对，它只存Key用于查找，故branch page中实际上没有存Key的Value，
	// 所以branchPageElement中并没像leafPageElement那样定义了vsize，因为branchPageElement的vsize始终是0。
	for i, item := range n.inodes {
		_assert(len(item.key) > 0, "write: zero-length inode key")

		// Write the page element
		if n.isLeaf {
			// 定义leafPageElement指针，指向page中第i个leafPageElement
			// 当index=0，第一个leafPageElement是从p.ptr指向的位置开始存的，而且所有leafPageElement是连续存储的
			elem := p.leafPageElement(uint16(i))
			// pos的值为b[0]到当前leafPageElement的偏移
			// 当i=0时，b[0]实际上就是页中最后一个element的结尾处。一次for循环，实际上就是写入一个element和一个K/V对。
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.flags = item.flags
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.pgid = item.pgid
			_assert(elem.pgid != p.id, "write: circular dependency occurred")
		}

		// If the length of key+value is larger than the max allocation size
		// then we need to reallocate the byte array pointer.
		//
		// See: https://github.com/boltdb/bolt/pull/335
		klen, vlen := len(item.key), len(item.value)
		if len(b) < klen+vlen {
			b = (*[maxAllocSize]byte)(unsafe.Pointer(&b[0]))[:]
		}

		// Write data for the element to the end of the page.
		// 将key写入b
		copy(b[0:], item.key)
		b = b[klen:]
		// 将key写入
		copy(b[0:], item.value)
		b = b[vlen:]
	}

	// DEBUG ONLY: n.dump()
}

// split breaks up a node into multiple smaller nodes, if appropriate.
// This should only be called from the spill() function.
// 实际上就是把node分成两段，其中一段满足node要求的大小，另一段再进一步按相同规则分成两段，一直到不能再分为止
func (n *node) split(pageSize int) []*node {
	var nodes []*node

	node := n
	for {
		// Split node into two.
		a, b := node.splitTwo(pageSize)
		nodes = append(nodes, a)

		// If we can't split then exit the loop.
		if b == nil {
			break
		}

		// Set node to b so it gets split on the next iteration.
		node = b
	}

	return nodes
}

// splitTwo breaks up a node into two smaller nodes, if appropriate.
// This should only be called from the split() function.
func (n *node) splitTwo(pageSize int) (*node, *node) {
	// Ignore the split if the page doesn't have at least enough nodes for
	// two pages or if the nodes can fit in a single page.
	// 节点分裂的条件:
	//		1) 节点大小超过了页大小
	//		2) 节点Key个数大于每节点Key数量最小值的两倍，这是为了保证分裂出的两个节点中的Key数量都大于每节点Key数量的最小值;
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		return n, nil
	}

	// Determine the threshold before starting a new node.
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	// 分裂的门限值
	threshold := int(float64(pageSize) * fillPercent)

	// Determine split position and sizes of the two pages.
	// 根据门限值计算分裂的位置
	splitIndex, _ := n.splitIndex(threshold)

	// Split node into two separate nodes.
	// If there's no parent then we'll need to create one.
	// 如果要分裂的节点没有父节点(可能是根节点)，则应该新建一个父node，同时将当前节点设为它的子node
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}

	// Create a new node and add it to the parent.
	// 创建了一个新node，并将当前node的父节点设为它的父节点
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)

	// Split inodes across two nodes.
	// 将当前node的从分裂位置开始的右半部分记录拷贝给新node;
	next.inodes = n.inodes[splitIndex:]
	// 将当前node的记录更新为原记录集合从分裂位置开始的左半部分，从而实现了将当前node一分为二;
	n.inodes = n.inodes[:splitIndex]

	// Update the statistics.
	n.bucket.tx.stats.Split++

	return n, next
}

// 分裂位置要同时保证:
//		1. 前半部分的节点数量大于每节点Key数量最小值(minKeysPerPage);
//		2. 后半部分的节点数量大于每节点Key数量最小值(minKeysPerPage);
//		3. 分裂后半部分node的大小是不超过门限值的最小值，即前半部分的size要在门限范围内尽量大;
// splitIndex finds the position where a page will fill a given threshold.
// It returns the index as well as the size of the first page.
// This is only be called from split().
func (n *node) splitIndex(threshold int) (index, sz int) {
	sz = pageHeaderSize

	// Loop until we only have the minimum number of keys required for the second page.
	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = i
		inode := n.inodes[i]
		elsize := n.pageElementSize() + len(inode.key) + len(inode.value)

		// If we have at least the minimum number of keys and adding another
		// node would put us over the threshold then exit and return.
		if i >= minKeysPerPage && sz+elsize > threshold {
			break
		}

		// Add the element size to the total size.
		sz += elsize
	}

	return
}

// node的spill过程与rebalance过程不同，rebalance是从当前节点到根节点递归，而spill是从根节点到叶子节点进行递归，不过它们最终都要处理根节点的rebalance或者spill
// 在spill中，如果根节点需要分裂(如上图中的第二步)，则需要对其递归调用spill，但是为了防止循环，调用父节点的spill之前，会将父node中缓存的子节点
// 引用集合children置空，以防止向下递归。
// spill writes the nodes to dirty pages and splits nodes as it goes.
// Returns an error if dirty pages cannot be allocated.
func (n *node) spill() error {
	var tx = n.bucket.tx
	// 检查当前node是否已经spill过，如果已经spill过，则无需spill
	if n.spilled { // (1)
		return nil
	}

	// Spill child nodes first. Child nodes can materialize sibling nodes in
	// the case of split-merge so we cannot use a range loop. We have to check
	// the children size on every loop iteration.
	// 对子节点进行深度优先访问并递归调用spill()，需要注意的是，子节点可能会分裂成多个节点，
	// 分裂出来的新节点也是当前节点的子节点，n.children这个slice的size会在循环中变化，帮不能使用rang的方式循环访问；
	// 同时，分裂出来的新节点会在代码(9)处被设为spilled，所以在代码(2)的下一次循环访问到新的子节点时不会重新spill，
	// 这也是代码(1)处对spilled进行检查的原因;
	sort.Sort(n.children)
	for i := 0; i < len(n.children); i++ { // (2)
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}

	// We no longer need the child list because it's only used for spill tracking.
	// 完成spill 后，将子节点集合children设置为空，以防向上递归掉员工spill的时候形成回路
	n.children = nil

	// Split nodes into appropriate sizes. The first node will always be n.
	// 调用node的split方法按页大小将node分裂出若干新node，新node与当前node共享一个父node，返回的nodes中包含当前node
	var nodes = n.split(tx.db.pageSize)
	// 处理分裂后产生的node
	for _, node := range nodes {
		// Add node's page to the freelist if it's not new.
		if node.pgid > 0 {
			// 释放当前node的所占页，因为随后要为它分配新的页，我们前面说过transaction commit是只会向磁盘写入当前transaction分配的脏页，
			// 所以这里要对当前node重新分配页;
			tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
			node.pgid = 0
		}

		// Allocate contiguous space for the node.
		// 调用Tx的allocate()方法为分裂产生的node分配页缓存，请注意，通过splite()方法分裂node后，node的大小为页大小 * 填充率，
		// 默认填充率为50%，而且一般地它的值小于100%，所以这里为每个node实际上是分配一个页框;
		p, err := tx.allocate((node.size() / tx.db.pageSize) + 1)
		if err != nil {
			return err
		}

		// Write the node.
		if p.id >= tx.meta.pgid {
			panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", p.id, tx.meta.pgid))
		}
		// 将新node的页号设为分配给他的页框的页号
		node.pgid = p.id
		// 将新node序列化并写入刚刚分配的页缓存;
		node.write(p)
		node.spilled = true // (9)

		// Insert into parent inodes.
		// 向父节点更新或添加Key和Pointer，以指向分裂产生的新node
		if node.parent != nil { // 如果不是根节点
			// 将父node的key设为第一个子node的第一个key;
			var key = node.key // split后，最左边node
			if key == nil { // split后，非最左边node
				key = node.inodes[0].key
			}
			// 向父node写入Key和Pointer，其中Key是子结点的第一个key，Pointer是子节点的pgid;
			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0)
			// 将分裂产生的node的key设为其中的第一个key;
			node.key = node.inodes[0].key
			_assert(len(node.key) > 0, "spill: zero-length node key")
		}

		// Update the statistics.
		tx.stats.Spill++
	}

	// 从根节点处递归完所有子节点的spill过程后，若根节点需要分裂，则它分裂后将产生新的根节点，代码(13)和(14)对新产生的根节点进行spill;
	// If the root node split and created a new root then we need to spill that
	// as well. We'll clear out the children to make sure it doesn't try to respill.
	if n.parent != nil && n.parent.pgid == 0 {
		n.children = nil
		return n.parent.spill()
	}

	return nil
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
func (n *node) rebalance() {
	// 只有unbalanced为true是才进行节点再平衡，只有当节点中有过删除操作时，unbalanced才为true
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// Update statistics.
	n.bucket.tx.stats.Rebalance++

	// Ignore if node is above threshold (25%) and has enough keys.
	// 再平衡条件检查，只有当节点存的KV总大小小于页大小的25%且节点中key的数量少于设定的节点最小Key数量值时，才会进行旋转
	var threshold = n.bucket.tx.db.pageSize / 4
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
		return
	}

	// Root node has special handling.
	if n.parent == nil {
		// If root node is a branch and only has one node then collapse it.
		// 根节点只有一个子节点的情形，将子节点上的inodes拷贝到根节点上，并将子节点上的inodes拷贝到根节点上
		// 并将子节点的所有孩子移交给根节点，并将孩子节点的父节点更新为根节点；如果子节点是一个叶子节点，
		// 则将子节点的inodes全部拷贝到根节点上后，根节点也是一个叶子节点；最后，将子节点删除;
		if !n.isLeaf && len(n.inodes) == 1 { // (3)
			// Move root's child up.
			child := n.bucket.node(n.inodes[0].pgid, n)
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]
			n.children = child.children

			// Reparent all child nodes being moved.
			for _, inode := range n.inodes {
				if child, ok := n.bucket.nodes[inode.pgid]; ok {
					child.parent = n
				}
			}

			// Remove old child.
			child.parent = nil
			delete(n.bucket.nodes, child.pgid)
			child.free()
		}

		return
	}

	// If node has no keys then just remove it.
	// 如果节点变成一个空节点，则将它从B+Tree中删除，并把父节点上的Key和Pointer删除，由于父节点上有删除，得对父节点进行再平衡;
	if n.numChildren() == 0 {
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
		n.parent.rebalance()
		return
	}

	_assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// Destination node is right sibling if idx == 0, otherwise left sibling.
	var target *node
	// 下面的代码是合并兄弟节点与当前节点中的记录集合逻辑
	// 代码(3)处决定是合并左节点还是右节点：如果当前节点是父节点的第一个孩子，则将右节点中的记录合并到当前节点中；如果当前节点是父节点的第二个
	// 或以上的节点，则将当前节点中的记录合并到左节点中;
	var useNextSibling = (n.parent.childIndex(n) == 0)
	if useNextSibling {
		target = n.nextSibling()
	} else {
		target = n.prevSibling()
	}

	// If both this node and the target node are too small then merge them.
	// 将右节点中记录合并到当前节点：首先将右节点的孩子节点全部变成当前节点的孩子，右节点将所有孩子移除；随后，将右节点中的记录全部拷贝到当前节点；
	// 最后，将右节点从B+Tree中移除，并将父节点中与右节点对应的记录删除;
	if useNextSibling {
		// Reparent all child nodes being moved.
		for _, inode := range target.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = n
				child.parent.children = append(child.parent.children, child)
			}
		}

		// Copy over inodes from target and remove target.
		n.inodes = append(n.inodes, target.inodes...)
		n.parent.del(target.key)
		n.parent.removeChild(target)
		delete(n.bucket.nodes, target.pgid)
		target.free()
	} else {
		// Reparent all child nodes being moved.
		// 将当前节点中的记录合并到左节点
		for _, inode := range n.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = target
				child.parent.children = append(child.parent.children, child)
			}
		}

		// Copy over inodes to target and remove node.
		target.inodes = append(target.inodes, n.inodes...)
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
	}

	// Either this node or the target node was deleted from the parent so rebalance it.
	// 合并兄弟节点与当前节点时，会移除一个节点并从父节点中删除一个记录，所以需要对父节点进行再平衡，如代码(8)处所示，
	// 所以节点的rebalance也是一个递归的过程，它会从当前结点一直进行到根节点处;
	n.parent.rebalance()
}

// removes a node from the list of in-memory children.
// This does not affect the inodes.
func (n *node) removeChild(target *node) {
	for i, child := range n.children {
		if child == target {
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

// dereference causes the node to copy all its inode key/value references to heap memory.
// This is required when the mmap is reallocated so inodes are not pointing to stale data.
func (n *node) dereference() {
	if n.key != nil {
		key := make([]byte, len(n.key))
		copy(key, n.key)
		n.key = key
		_assert(n.pgid == 0 || len(n.key) > 0, "dereference: zero-length node key on existing node")
	}

	for i := range n.inodes {
		inode := &n.inodes[i]

		key := make([]byte, len(inode.key))
		copy(key, inode.key)
		inode.key = key
		_assert(len(inode.key) > 0, "dereference: zero-length inode key")

		value := make([]byte, len(inode.value))
		copy(value, inode.value)
		inode.value = value
	}

	// Recursively dereference children.
	for _, child := range n.children {
		child.dereference()
	}

	// Update statistics.
	n.bucket.tx.stats.NodeDeref++
}

// free adds the node's underlying page to the freelist.
func (n *node) free() {
	if n.pgid != 0 {
		n.bucket.tx.db.freelist.free(n.bucket.tx.meta.txid, n.bucket.tx.page(n.pgid))
		n.pgid = 0
	}
}

// dump writes the contents of the node to STDERR for debugging purposes.
/*
func (n *node) dump() {
	// Write node header.
	var typ = "branch"
	if n.isLeaf {
		typ = "leaf"
	}
	warnf("[NODE %d {type=%s count=%d}]", n.pgid, typ, len(n.inodes))

	// Write out abbreviated version of each item.
	for _, item := range n.inodes {
		if n.isLeaf {
			if item.flags&bucketLeafFlag != 0 {
				bucket := (*bucket)(unsafe.Pointer(&item.value[0]))
				warnf("+L %08x -> (bucket root=%d)", trunc(item.key, 4), bucket.root)
			} else {
				warnf("+L %08x -> %08x", trunc(item.key, 4), trunc(item.value, 4))
			}
		} else {
			warnf("+B %08x -> pgid=%d", trunc(item.key, 4), item.pgid)
		}
	}
	warn("")
}
*/

type nodes []*node

func (s nodes) Len() int           { return len(s) }
func (s nodes) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool { return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1 }

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
type inode struct {
	flags uint32 // 指明改KV对是否代表一个Bucket，如果是Bucket，则其值为1，否则为0
	pgid  pgid // 根节点或内节点的子节点的pgid，可以理解为Pointer，请注意，叶子节点中该值无意义;
	key   []byte // KV对中的Key
	value []byte // KV对中的Value，非叶子节点中该值没有意义
}

type inodes []inode
