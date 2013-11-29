
# LRU Bounded Map

This repository contains several Haskell implementations of associative containers (maps / dictionaries) which retire elements in least recently used (LRU) order when growing past a specified limit (bounded).

# Use case

A two-level cache for images fetched over HTTP / from disk into system memory and then finally into GPU memory. Images are looked up far more frequently than they are inserted / deleted, most of the lookups should be cache hits. The cache is expected to be at capacity most of the time, the size of the images dwarfs any structural overhead from the cache. Required operations are insert, update, lookup, delete (all updating the LRU timestamp) and retrieval of the newest and oldest item.

# Implementations

With the requirements above in mind, there are four different implementations in various stages of completion, polish and optimization. All containers are purely functional data structures.

### LRUBoundedMap_DoubleMapBTree

Perhaps the most simplistic and obvious data structure satisfying the above requirements is a pair of maps, indexing the values with both the lookup keys and an incrementing tick to keep track of their age.

It's a bit tricky to keep both maps synchronized, and the important lookup operation needs five O(log n) operations for the lookup + LRU update. To facilitate retrieval of the oldest / newest element, at least the map containing the keys needs to be an ordered container, forcing us to chose the slower `Data.Map` over the faster `Data.HashMap`.

The double map on top of a `Data.Map.Strict` is simple, but the slowest of the solutions evaluated here.

### LRUBoundedMap_LinkedListHashMap

Inspired by the [lrucache package](http://hackage.haskell.org/package/lrucache), this container maintains a doubly linked-list over the entries of the map. Like the double map, it also needs five O(log n) operations for a lookup + reshuffling of the linked-list during the LRU update. Since there is no requirement for the underlying map to be ordered anymore, we can now use `Data.HashMap.Strict` for a speed boost.

Note that the size operation of the hash map is O(n) instead of the regular map's O(1). This causes the insert operation of our bounded map to be O(n) as well, as it has to check if the size of the map has crossed the limit after the insertion. This could easily be fixed by maintaining a separate size field (as done by the two [Trie](http://hackage.haskell.org/package/containers-0.5.3.1/docs/Data-IntMap-Strict.html#v:updateLookupWithKey) based containers).

Possible optimization, besides a constant-time size operation, would be implementing an underlying hash map container supporting a [updateLookupWithKey](http://hackage.haskell.org/package/containers-0.5.3.1/docs/Data-IntMap-Strict.html#v:updateLookupWithKey) function and re-use of hashed key values between operations. Also, if implemented on top of a mutable array hash table with indices / pointers instead of keys for the linked-list links, the performance would likely be far better than any of the purely functional data structures evaluated here (1x O(1) lookup + 5x updating pointer).

The implementation of this container is a bit more polished and optimized than the prior double map.

### LRUBoundedMap_CustomHAMT

Haskell's `Data.HashMap` is implemented as a [Hash Array Mapped Trie (HAMT)](http://en.wikipedia.org/wiki/Hash_array_mapped_trie), and this is an attempt to extend this data structure to support LRU / MRU retrieval. Only an attempt, because some early benchmarks and tests indicated that a bitwise Trie seems like a better choice here. For approximate-LRU behavior, [this Reddit post](http://www.reddit.com/r/haskell/comments/1qt9f3/writing_a_better_bounded_lru_map/cdginlw) has some interesting ideas.

The current implementation does not implement any LRU / bounded behavior so far, and is basically a 16-way HAMT without the bitmap indexing for partially filled nodes present in `Data.HashMap`.

### LRUBoundedMap_CustomHashedTrie

This final container uses [Bitwise Trie](http://en.wikipedia.org/wiki/Trie#Bitwise_tries) / [Hash Tree](http://en.wikipedia.org/wiki/Hash_tree_%28persistent_data_structure%29) instead of an HAMT, making it a bit easier to maintain the LRU / MRU bounds inside the tree nodes.

Like the double map, this container uses an incrementing tick to timestamp its entries. One issue to deal with is the overflow of the tick. Using a 64 bit tick is sufficient to guarantee it will never overflow, but creates a fair bit of memory overhead and a significant performance drop on 32 bit GHC, as it has very poorly performing support for 64 bit operations. The latter could be addressed by abusing a Double as a 53 bit integer, but a better overall solution seems to be to simply compact the tick range by rebuilding the map every time the 32 bit tick overflows (rather infrequently).

This container is the fastest / most polished and the one I actually ended up using for my use case.

### Other

If the performance of the containers here is not sufficient, the two most promising ideas seem to be the mutable linked-list hash table as described above, or this interesting map + MRU list data structure: [Reddit post](http://www.reddit.com/r/haskell/comments/1qt9f3/writing_a_better_bounded_lru_map/cdjb39u) / [Direct link to code](http://lpaste.net/95955).

# Benchmark

### Setup

[Criterion](http://hackage.haskell.org/package/criterion) has been used as the benchmarking framework. 

The file `keys.txt` contains 5000 URL strings like this one

    http://abs.twimg.com/sticky/default_profile_images/default_profile_0_bigger.png

which are used as `ByteString` keys during the benchmark. Care is taken that at least one hash collision exists among the keys. Values are simple Ints.

The benchmarked operations are insertion, deletion and lookup.

### Results

Some important notes on interpreting the results:

* All measurements performed on a 2.26Ghz Core 2 Duo, compiled with GHC 7.6.3 / HP 2013.2.0.0 

* The optimization effort put in differs between the containers, and priorities have been determined by the specific [use case](#use-case)

* Timings are always for performing the operation with all 5000 keys

* The `(lim 1k)` and `(lim 5k)` labels indicate the element limit specified for the container. Regardless, all benchmarks still measure time for 5000 keys. For instance, the insertion benchmarks are faster for limit 5k as the limit 1k container will have to retire old elements during insertion. The lookup benchmarks are faster for limit 1k, as the limit 5k will have to perform 4000 additional LRU updates for keys already retired from the smaller container

* `Data.Map.Strict`, `Data.HashMap.Strict` and `Data.IntMap.Strict` were included as a speed-of-light reference, even though they do not support any kind of LRU / bounded behavior. Some of our custom containers have non-LRU updating versions of their operations and are also included in those benchmark groups. The HAMT data structure has an incomplete implementation of the LRU / bounded behavior, and is only measured as such. Benchmarks are labelled as `w/ LRU upd` or `w/o LRU upd` to indicate which behavior is active during the measurement

* Insertion benchmarks for the linked-list hash map are omitted as they run very slowly (see description in [Implementations](#lruboundedmap_linkedlisthashmap))

* The order of the keys for delete / lookup operations is a random permutation of their insertion order, except for the `lookup (w/ LRU upd, ord keys)` benchmarks. Those exist to show how the linked-list hash map gives an unrealistic result if keys are looked up in insertion order, as moving the oldest element to the front of LRU linked-list is faster than doing so with items from the middle of the list

![Benchmarks](https://raw.github.com/blitzcode/lru-bounded-map/master/benchmarks.png)

See the [full Criterion report](http://htmlpreview.github.io/?https://raw.github.com/blitzcode/lru-bounded-map/master/report.html) from `report.html`.

# Other useful bits

Two pieces of code which might useful outside of the LRU bounded map implementations:

* `FisherYatesShuffle.hs` contains an implementation of the famous permutation algorithm
* `DoubleMap.hs` is the two-key map used internally by `LRUBoundedMap_DoubleMapBTree`

# Legal

This program is published under the [MIT License](http://en.wikipedia.org/wiki/MIT_License).

# Author

Developed by Tim C. Schroeder, visit my [website](http://www.blitzcode.net) to learn more.

