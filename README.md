# Assignment1 - COMP5349

## How to run

The assignment is divided into 4 separate tasks in this implementation:

* __Task0__: Join - Join photo and place together
* __Task1__: Output place_name numOfPhotos
* __Task2__: Output top50 result in Task1
* __Task3__: Output top50 reuslt in Task1 along with its top10 tags. place_name numOfPhotos (tag: freq)+ 


To run the project:

```shell
# Task0 - Join
hadoop jar PlaceFilter.jar task3.JoinDriver /share/place.txt /share/photo/* Task0
```

```shell
# Task1.1 - raw output with tags
hadoop jar PlaceFilter.jar task3.PlaceFilterDriver Task0/* Task1

# Task1.2
cat Task1/* | cut -f 1,2
```

```shell
# Task2
hadoop jar PlaceFilter.jar task3.KeyChangeDriver Task1/* Task2
```

```shell
# Task3 - run in total
hadoop jar PlaceFilter.jar task3.Task3Driver Task1/* Task2/* Task3

# Task3.1 
hadoop jar PlaceFilter.jar task3.TagDriver Task1/* Task3.1

# Task3.2
hadoop jar PlaceFilter.jar task3.TagFrequentDriver Task3.1/* Task3.2

# Task3.3
hadoop jar PlaceFilter.jar task3.TopTagDriver Task3.2/* Task3.3

# Task3.4
hadoop jar PlaceFilter.jar task3.TopTagPlaceJoinDriver Task2/* Task3.3/* Task3.4

# Task3.5 - Task3.5 is the final result output folder.
hadoop jar PlaceFilter.jar task3.PlaceNumTagDriver Task3.4/* Task3.5
```



## Design of the project

### Task1

Most important thing first, the project firstly join the photo file in `/share/photo` and place file which is `/share/place.txt` together, having the desired data fields (`place id`, `place_name`, `locality-type-id`,`tags` ) left in the newly generated data.

After joining, we simply get photos with locality level of 7 and 22 (have to refine the `place_name`), and get data fields (`place_name`, filtered `tags`) what we desire. Here, `place_name` is the natural key and we do map/reduce work on these data sets to get count of `place_name`, which is the number of photos in each `place_name`



### Task2

With the output file of _Task1_, the idea comes out straight. We just sort outputs in _Task1_ and output the first 50 one, which means we should use `numOfPhotos` as natural key and have a comparator for it (since it's now `hadoop.Text`). And this is what `KeyChangeMapper`, `KeyChangeReducer` and `KeyInputComparator` do.



### Task3

Similar to the idea in _Task2_, we firstly get the top 50 place __with its all tags__. This can be done in the same way as _Task2_ (here it is `TagDriver`, `TagMapper` and `TagReducer`).

After that, we have to map the above output so that we can put single `tag` and its corresponding `place_name` as the natural key (and yes, we define `TextTextPair` class for it). And then, reduce it so we can get the total number of each `(place_name, tag)` pair. This part is pretty resource consuming. Hence, we add a combiner to reduce the load. This job is done by `TagFrequentMapper`, `TagFrequentCombiner` and `TagFrequentReducer`.

With this, the idea goes back to _Task2_, we have to sort the `(place_name, tag)`, but for each `place_name`. To achieve this, we have a very tricky design, we map the above output as `(place_name, freq),tag\tfreq `. In this way, we firstly sort according to `freq` and then group the result according to `place_name`. That is, in the input of reducer, the same `place_name` with different `freq` should be grouped together. In this way, once we only output the first 10 values for each `place_name`, we could derive the result.  `TopTagMapper`. `TopTagReducer`, `TopTagSortComparator`, `TopTagGroupingComparator` and `TopTagPartitioner` are for this job.

At the end, the `numOfPhotos` column is missing and the output should be sorted according to it. Repeat the same technique in joining and sorting by `numOfPhotos` as natural key. We join result in _Task2_ and sort it. Then the final result comes out. See `TopTagPlaceJoinDriver` and `PlaceNumTagDriver`.