# Assignment1 - COMP5349

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
# Task1
hadoop jar PlaceFilter.jar task3.PlaceFilterDriver Task0/* Task1
```

```shell
# Task2
hadoop jar PlaceFilter.jar task3.KeyChangeDriver Task1/* Task2
```

```shell
# Task3

# Task3.1 
hadoop jar PlaceFilter.jar task3.TagDriver Task1/* Task3.1

# Task3.2
hadoop jar PlaceFilter.jar task3.TagFrequentDriver Task3.1/* Task3.2

# Task3.3
hadoop jar PlaceFilter.jar task3.TopTagDriver Task3.2/* Task3.3

# Task3.4
hadoop jar PlaceFilter.jar task3.TopTagPlaceJoinDriver Task2/* Task3.3/* Task3.4

# Task3.5
hadoop jar PlaceFilter.jar task3.PlaceNumTagDriver Task3.4/* Task3.5
```

Task3.5 is the final result output folder.