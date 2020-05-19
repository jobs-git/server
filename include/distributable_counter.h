#pragma once

#include <cassert>

#include <atomic>
#include <mutex>

#include "intrusive_list.h"

namespace maria
{

template <typename Integral> class counter_broker;

// Increment this class directly or distribute with several brokes to
// increase scalability.
// Class is optimized to writes. Reads are O(N) where N is a number of brokers.
template <typename Integral> class distributable_counter
{
  distributable_counter(const distributable_counter &)= delete;
  distributable_counter &operator=(const distributable_counter &)= delete;
  distributable_counter(distributable_counter &&)= delete;
  distributable_counter &operator=(distributable_counter &&)= delete;

public:
  explicit constexpr distributable_counter(Integral initial)
      : counter_(initial)
  {
  }
  constexpr distributable_counter() {}
  ~distributable_counter() { assert(brokers_.empty()); }

  void operator+=(Integral amount)
  {
    counter_.fetch_add(amount, std::memory_order_relaxed);
  }
  void operator-=(Integral amount)
  {
    counter_.fetch_sub(amount, std::memory_order_relaxed);
  }
  void operator++() { *this+= 1; }
  void operator++(int) { *this+= 1; }
  void operator--() { *this-= 1; }
  void operator--(int) { *this-= 1; }

  Integral load();
  // You can reset a value to avoid overflow or for some other reason.
  Integral exchange(Integral to);

private:
  std::atomic<Integral> counter_;
  intrusive::list<counter_broker<Integral>> brokers_; // guarded by mutex_
  std::mutex mutex_;

  friend counter_broker<Integral>;
};

// Write only class. You can read a value only from distributor_counter.
// Make it local variable or thread_local variable or put it into array
// to distribute with %count technique.
template <typename Integral>
class counter_broker : public intrusive::list_node<>
{
  counter_broker(const counter_broker &)= delete;
  counter_broker &operator=(const counter_broker &)= delete;
  counter_broker(counter_broker &&)= delete;
  counter_broker &operator=(counter_broker &&)= delete;

public:
  counter_broker(distributable_counter<Integral> &counter) : counter_(counter)
  {
    std::lock_guard<std::mutex> _(counter.mutex_);
    counter.brokers_.push_back(*this);
  }

  ~counter_broker()
  {
    counter_+= amount_.load(std::memory_order_relaxed);
    std::lock_guard<std::mutex> _(counter_.mutex_);
    counter_.brokers_.remove(*this);
  }

  void operator+=(Integral amount)
  {
    amount_.fetch_add(amount, std::memory_order_relaxed);
  }
  void operator-=(Integral amount)
  {
    amount_.fetch_sub(amount, std::memory_order_relaxed);
  }
  void operator++() { *this+= 1; }
  void operator++(int) { *this+= 1; }
  void operator--() { *this-= 1; }
  void operator--(int) { *this-= 1; }

private:
  std::atomic<Integral> amount_;
  distributable_counter<Integral> &counter_;

  friend distributable_counter<Integral>;
};

template <typename Integral> Integral distributable_counter<Integral>::load()
{
  Integral accumulator= 0;
  {
    std::lock_guard<std::mutex> _(mutex_);
    for (const auto &broker : brokers_)
      accumulator+= broker.amount_.load(std::memory_order_relaxed);
  }
  return accumulator + counter_.load(std::memory_order_relaxed);
}

template <typename Integral>
Integral distributable_counter<Integral>::exchange(Integral to)
{
  Integral old_value= 0;

  {
    std::lock_guard<std::mutex> _(mutex_);
    for (auto &broker : brokers_)
      old_value+= broker.amount_.exchange(0, std::memory_order_relaxed);
  }

  return old_value+= counter_.exchange(to, std::memory_order_relaxed);
}

} // namespace maria
