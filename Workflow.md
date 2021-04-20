One principle, for every topic we produced, such as source, id or name, I use 4 * 4 partions to produce, and every partion using one go routing.

# Produce Random Source

I make a generator for generating one formatted data once a call. For every partion of topic "source", I calculated nums of data that this partion can save, and run a go routine with different sarama producer to produce random generated csv data.

# Split Data and Save to Sub Topic

If we have n lines of data, let we split data with p parts, it means we will handle n / p lines of data every time. Take topic "id" as example, p is also the same as numbers of sub topic, topic name range from [id1, id2, id3, ..., idp].

For every partion of topic "source", I make a sarama consume partition , and run a go routing to read, all the go routing read n / p lines in total.

Then I sort these data with key "id", and save it to sub topic "id1", with 4 * 4 partions, just the same as topic "source", every partion has a go routing. The data will be distributed in the following format. Data is ordered from partion1 to partionp

![id1](https://github.com/tanyifeng/kafkasort/blob/master/id1.png)

Then continue sort these data with key "name", save it to sub topic "name1", the same as topic "id1", then sort and save with key "continent".

Continue flow described above, sort and saving id2, name2, continent2, id3, ..., idp, namep, continentp.

# Merge Splited Data

For each topic of ["id", "name", "continent"], I run a go routing to merge, such as topic "id".

Creating an producer of topic "id", read one message from every sub topic of id, we get an array with p lines of message, find the minimum message in this array, produce it to topic "id", if the minimum message comes from idx, we read next message of idx, save to array again.

Continue this merge flow  to the end and finish the whole sort.
