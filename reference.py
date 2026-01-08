"""
playground.py - Refactored OOP Classes
======================================
Contains: Counter, FrequencyCounter, MessageQueue

Each class includes:
- Full type hints
- Proper docstrings
- Edge case handling
- Clean, predictable semantics
"""

from typing import Any, Optional
from collections import defaultdict


class Counter:
    """
    A running total accumulator.
    
    Tracks the cumulative sum of numbers added over time.
    Useful for streaming sums without storing all values.
    
    Attributes:
        _total (float): Internal running sum.
    
    Example:
        >>> counter = Counter()
        >>> counter.add(5)
        >>> counter.add(3)
        >>> counter.total()
        8.0
    """
    
    def __init__(self) -> None:
        """Initialize counter with zero total."""
        self._total: float = 0.0
    
    def add(self, value: float) -> None:
        """
        Add a number to the running total.
        
        Args:
            value: Number to add. Can be int or float.
        
        Raises:
            TypeError: If value is not a number.
        """
        if not isinstance(value, (int, float)):
            raise TypeError(f"Expected number, got {type(value).__name__}")
        self._total += value
    
    def total(self) -> float:
        """
        Get the current cumulative sum.
        
        Returns:
            The sum of all values added so far.
        """
        return self._total
    
    def reset(self) -> None:
        """Reset the counter to zero."""
        self._total = 0.0
    
    def __repr__(self) -> str:
        return f"Counter(total={self._total})"


class FrequencyCounter:
    """
    Tracks occurrence counts of hashable values.
    
    Stores how many times each value has been added.
    Supports querying counts and finding most/least common values.
    
    Attributes:
        _counts (dict): Maps value -> occurrence count.
    
    Notes:
        - Keys are case-sensitive (e.g., 'A' != 'a')
        - Any hashable type accepted (int, str, tuple, etc.)
        - Results are NOT sorted by default
    
    Example:
        >>> fc = FrequencyCounter()
        >>> fc.add("apple")
        >>> fc.add("banana")
        >>> fc.add("apple")
        >>> fc.get_count("apple")
        2
    """
    
    def __init__(self) -> None:
        """Initialize with empty frequency map."""
        self._counts: dict[Any, int] = defaultdict(int)
    
    def add(self, value: Any) -> None:
        """
        Increment count for a value.
        
        Args:
            value: Any hashable value to count.
        """
        self._counts[value] += 1
    
    def get_count(self, value: Any) -> int:
        """
        Get occurrence count for a value.
        
        Args:
            value: The value to look up.
        
        Returns:
            Number of times value was added. Returns 0 if never added.
        """
        return self._counts.get(value, 0)
    
    def most_common(self, k: int = 1) -> list[tuple[Any, int]]:
        """
        Get the k most frequently occurring values.
        
        Args:
            k: Number of top values to return. Defaults to 1.
        
        Returns:
            List of (value, count) tuples, sorted by count descending.
            Returns empty list if no values tracked.
        
        Example:
            >>> fc.most_common(2)
            [('apple', 3), ('banana', 2)]
        """
        if not self._counts:
            return []
        
        sorted_items = sorted(
            self._counts.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return sorted_items[:k]
    
    def all_with_max_frequency(self) -> list[Any]:
        """
        Get all values that share the highest frequency.
        
        Returns:
            List of values with max count. Empty if no values tracked.
        """
        if not self._counts:
            return []
        
        max_count = max(self._counts.values())
        return [val for val, count in self._counts.items() if count == max_count]
    
    def unique_count(self) -> int:
        """Return number of distinct values tracked."""
        return len(self._counts)
    
    def total_count(self) -> int:
        """Return total number of items added (including duplicates)."""
        return sum(self._counts.values())
    
    def __repr__(self) -> str:
        return f"FrequencyCounter({dict(self._counts)})"


class EmptyQueueError(Exception):
    """Raised when attempting to dequeue from an empty queue."""
    pass


class MessageQueue:
    """
    A FIFO message queue with optional capacity limit.
    
    Messages are processed in the order they were added.
    Supports optional max capacity to prevent unbounded growth.
    
    Attributes:
        _messages (list): Internal message storage.
        _capacity (int | None): Max messages allowed, or None for unlimited.
    
    Notes:
        - dequeue() raises EmptyQueueError on empty queue
        - enqueue() raises OverflowError if at capacity
        - Messages can be any type (str recommended)
    
    Example:
        >>> mq = MessageQueue(capacity=100)
        >>> mq.enqueue("Hello")
        >>> mq.enqueue("World")
        >>> mq.dequeue()
        'Hello'
    """
    
    def __init__(self, capacity: Optional[int] = None) -> None:
        """
        Initialize the message queue.
        
        Args:
            capacity: Maximum number of messages. None for unlimited.
        
        Raises:
            ValueError: If capacity is zero or negative.
        """
        if capacity is not None and capacity <= 0:
            raise ValueError("Capacity must be positive")
        
        self._messages: list[Any] = []
        self._capacity: Optional[int] = capacity
    
    def enqueue(self, message: Any) -> None:
        """
        Add a message to the back of the queue.
        
        Args:
            message: The message to add.
        
        Raises:
            OverflowError: If queue is at capacity.
        """
        if self._capacity is not None and len(self._messages) >= self._capacity:
            raise OverflowError(f"Queue at capacity ({self._capacity})")
        
        self._messages.append(message)
    
    def dequeue(self) -> Any:
        """
        Remove and return the front message.
        
        Returns:
            The oldest message in the queue.
        
        Raises:
            EmptyQueueError: If queue is empty.
        """
        if self.is_empty():
            raise EmptyQueueError("Cannot dequeue from empty queue")
        
        return self._messages.pop(0)
    
    def peek(self) -> Any:
        """
        View the front message without removing it.
        
        Returns:
            The oldest message in the queue.
        
        Raises:
            EmptyQueueError: If queue is empty.
        """
        if self.is_empty():
            raise EmptyQueueError("Cannot peek empty queue")
        
        return self._messages[0]
    
    def is_empty(self) -> bool:
        """Check if queue has no messages."""
        return len(self._messages) == 0
    
    def size(self) -> int:
        """Return current number of messages in queue."""
        return len(self._messages)
    
    def __len__(self) -> int:
        """Support len() builtin."""
        return self.size()
    
    def __repr__(self) -> str:
        cap_str = f", capacity={self._capacity}" if self._capacity else ""
        return f"MessageQueue(size={self.size()}{cap_str})"


# =============================================================================
# DEMONSTRATION
# =============================================================================

def main() -> None:
    """Demonstrate all three classes with sample usage."""
    
    print("=" * 60)
    print("COUNTER DEMO")
    print("=" * 60)
    
    counter = Counter()
    for num in [10, 20, 30, -5]:
        counter.add(num)
        print(f"  Added {num:>3} → Total: {counter.total()}")
    
    print(f"\nFinal: {counter}")
    counter.reset()
    print(f"After reset: {counter}")
    
    print("\n" + "=" * 60)
    print("FREQUENCY COUNTER DEMO")
    print("=" * 60)
    
    fc = FrequencyCounter()
    words = ["apple", "banana", "apple", "cherry", "banana", "apple"]
    
    for word in words:
        fc.add(word)
    
    print(f"\nWords added: {words}")
    print(f"Unique values: {fc.unique_count()}")
    print(f"Total items: {fc.total_count()}")
    print(f"\nCount of 'apple': {fc.get_count('apple')}")
    print(f"Count of 'grape': {fc.get_count('grape')}")
    print(f"\nTop 2 most common: {fc.most_common(2)}")
    print(f"All with max frequency: {fc.all_with_max_frequency()}")
    
    print("\n" + "=" * 60)
    print("MESSAGE QUEUE DEMO")
    print("=" * 60)
    
    mq = MessageQueue(capacity=5)
    
    messages = ["First", "Second", "Third"]
    for msg in messages:
        mq.enqueue(msg)
        print(f"  Enqueued '{msg}' → Size: {mq.size()}")
    
    print(f"\nPeek: '{mq.peek()}'")
    print(f"Dequeue: '{mq.dequeue()}'")
    print(f"After dequeue, size: {mq.size()}")
    print(f"Is empty: {mq.is_empty()}")
    
    # Demonstrate error handling
    print("\nEmptying queue...")
    while not mq.is_empty():
        print(f"  Dequeued: '{mq.dequeue()}'")
    
    print(f"\nAttempting dequeue on empty queue...")
    try:
        mq.dequeue()
    except EmptyQueueError as e:
        print(f"  Caught error: {e}")
    
    print("\n" + "=" * 60)
    print("ALL DEMOS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
