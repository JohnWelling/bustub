//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
// 参考：https://blog.csdn.net/Altair_alpha/article/details/127547892
//      https://zhuanlan.zhihu.com/p/622663820
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <iostream>

#include "common/exception.h"
#include "common/rwlatch.h"

using namespace std;

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char)
  {
    key_char_ = key_char;
    is_end_ = false;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  // 需要注意的是移动构造函数 move constructor，由于unique_ptr表示独占资源，没有拷贝构造函数，
  // 所以这里使用move。key_char_和is_end_是简单的内置类型，没必要使用move构造
  TrieNode(TrieNode &&other_trie_node) noexcept
    : key_char_(other_trie_node.key_char_),
    is_end_(other_trie_node.is_end_),
    children_(move(other_trie_node.children_))
  {
    // 为什么InsertChildNode中的move操作没有调用移动构造函数
    std::cout << "move TrieNode" << std::endl;
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const 
  {
    auto value = children_.find(key_char);
    if (value == children_.end()) {
      return false;
    } 
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  // 此时为const修饰成员函数，要求const函数不能修改成员变量，否则会报错
  bool HasChildren() const 
  {
    if (children_.empty())
      return false;
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const 
  { 
    return is_end_;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child)
  {
    if (key_char != child->GetKeyChar() || HasChild(key_char))
      return nullptr;
    children_.emplace(key_char, move(child));
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char)
  {
    auto value = children_.find(key_char);
    if (value == children_.end()) {
      return nullptr;
    }
    return &(value->second);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) 
  {
    if(!HasChild(key_char))
      return;
    // 会自动释放智能指针指向的内存
    children_.erase(key_char);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) 
  {
    is_end_ = is_end;
  }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value): TrieNode(move(trieNode)), value_(value)
  {
    // is_end_是基类成员不能放在初始化列表中，放在函数体中赋值
    is_end_ = true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) :TrieNode(key_char) 
  {
    value_ = value;
    is_end_ = true;
  }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie()
  {
    root_ = make_unique<TrieNode>(' ');
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, T value) {
    if (key.empty()) {
      return false;
    }
    size_t count = 0;
    size_t str_len = key.size();
    unique_ptr<TrieNode> *node_ptr = &root_;
    latch_.WLock();
    for (auto ch : key) {
      count++;
      if (count < str_len)
      {
        unique_ptr<TrieNode> *tmp_node_ptr = node_ptr->get()->InsertChildNode(ch, std::make_unique<TrieNode>(ch));
        if (tmp_node_ptr == nullptr) {
          tmp_node_ptr = node_ptr->get()->GetChildNode(ch);
        }
        node_ptr = tmp_node_ptr;
      } else {
        unique_ptr<TrieNode> *end_node_ptr = node_ptr->get()->GetChildNode(ch);
        if (end_node_ptr == nullptr) {
          node_ptr->get()->InsertChildNode(ch, std::make_unique<TrieNodeWithValue<T>>(ch, value));
          latch_.WUnlock();
          return true;
        }
        if (end_node_ptr->get()->IsEndNode()) {
          latch_.WUnlock();
          return false;
        } else {
          // 这里为什么是TrieNodeWithValue<T>，需要看一下泛型的相关知识
          // unique_ptr的get函数，是智能指针的一个函数，可以获得其存储的指针
          auto new_node_ptr = new TrieNodeWithValue<T>(move(*(node_ptr->get())), value);
          // 会自动释放智能指针指向的内存
          node_ptr->reset(new_node_ptr);
          latch_.WUnlock();
          return true;
        }
      }
    }
    latch_.WUnlock();
    assert(false);
    // 上述应该包含所有情况，不应该走到这里
    return false;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  bool RemoveChildFromTrieNode(unique_ptr<TrieNode> *node_ptr, const std::string &key, size_t index)
  {
    if (index == key.size()) {
      return true;
    }
    if (!node_ptr->get()->HasChild(key[index])) {
      return false;
    }
    unique_ptr<TrieNode> *child_node_ptr = (*node_ptr)->GetChildNode(key[index]);
    if (!RemoveChildFromTrieNode(child_node_ptr, key, index+1)) {
      return false;
    }
    if (!child_node_ptr->get()->HasChildren()) {
      node_ptr->get()->RemoveChildNode(key[index]);
      return true;
    }
    if (index == key.size() - 1) {
      // 这里如果是将中间最终节点变为普通节点，无需转化，直接设置为false即可
      // 下次插入的时候，依据这个标记位直接替换也无影响
      child_node_ptr->get()->SetEndNode(false);
      return true;
    }
    // 如果走到这里应该是返回为true，证明路径上面没有可以删除的节点了
    return true;
  }

  bool Remove(const std::string &key) 
  {
    latch_.WLock();
    bool res = RemoveChildFromTrieNode(&root_, key, 0);
    latch_.WUnlock();
    return res;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    if (key.size() == 0) {
      *success = false;
      return {};
    }
    unique_ptr<TrieNode> *node_ptr = &root_;
    size_t index = 0;
    latch_.RLock();
    for (auto ch : key) {
      node_ptr = node_ptr->get()->GetChildNode(ch);
      if (node_ptr == nullptr) {
        *success = false;
        latch_.RUnlock();
        return {};
      }
      if (index == key.size() - 1) {
        auto term_node_ptr = dynamic_cast<TrieNodeWithValue<T> *>(node_ptr->get());
        if (term_node_ptr != nullptr) {
          *success = true;
          T result = term_node_ptr->GetValue();
          latch_.RUnlock();
          return result;
        } else {
          *success = false;
          latch_.RUnlock();
          return {};
        }
      }

      index++;
    }
    *success = false;
    latch_.RUnlock();
    return {};
  }
};
}  // namespace bustub
