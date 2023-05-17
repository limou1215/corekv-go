package utils

import (
	"math/rand"
	"sync"
	"bytes"
	"github.com/hardcore-os/corekv/utils/codec"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct { //跳表数据结构
	header *Element

	rand *rand.Rand

	maxLevel int // 跳表最大层数
	length   int //调表长度
	lock     sync.RWMutex
	size     int64 //
}

func NewSkipList() *SkipList {
	//implement me here!!!
	return &SkipList{}
}

type Element struct { // 每一个跳表节点   类似于 单链表节点
	levels []*Element   //多个节点后续 因为多个层级
	entry  *codec.Entry //就是咱们要存储得数据
	score  float64      //用于查找  取数据前八个字符进行计算分数
}

func newElement(score float64, data *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level), //创建一个指针数组 存放节点指针 最大几层 数组就为多大
		entry:  data,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error { // 添加一个节点
	//implement me here!!!
	score := list.calcScore(data.Key)
	var elem *Element
	max := len(list.header.levels)
	preElem := list.header
	var preElemHeaders [defaultMaxLevel]*Element
	for i := max - 1; i >= 0; i-- {
		preElemHeaders[i] = preElem
		for next := preElem.levels[i]; next != nil; next = preElem.levels[i] {
			if com := list.compare(score, data.Key, next); com <= 0 {
				if com == 0 {
					elem = next
					elem.entry = data
					return nil
				}
				break
			}
			preElem = next
			preElemHeaders[i] = preElem
		}
	}
	level := list.randLevel()
	elem = newElement(score, data, level)
	for i := 0; i < level; i++ {
		elem.levels[i] = preElemHeaders[i].levels[i]
		preElemHeaders[i].levels[i] = elem
	}
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) { // 寻找一个节点
	//implement me here!!!
	if list.length == 0 {
		return nil
	}
	score := list.calcScore(key)
	preElem := list.header
	i := len(list.header.levels) - 1
	for ; i >= 0; i-- {
		for next := preElem.levels[i]; next != nil; next = preElem.levels[i] {
			if comp := list.compare(score, key, next); comp <= 0 { // 如果当前值小于等于当前节点值 return -1
				if comp == 0 {
					return next.Entry()
				}
				break
			}
			preElem = next //说明当前值大于下一个节点值继续向前寻找
		}
	}
	return
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) { // 计算分值 加速比较
	var hash uint64
	l := len(key)

	if l > 8 { // only 前八位
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int { // 比较节点  分数加快查询
	//implement me here!!!
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score < next.score { // 和普通查找一样 如果当前节点小于下一个节点仍然没有找到
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int { // 用于向调表插入元素时候 插入哪一个level
	i := 1
	for ; i < list.maxLevel; i++ {
		if rand.Intn(2) == 0 {
			return i
		}
	}
	//implement me here!!!
	return i
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return 0
}
