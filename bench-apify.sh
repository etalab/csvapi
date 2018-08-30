wget http://localhost:8001/apify?url=http://datanova.legroupe.laposte.fr/explore/dataset/laposte_poincont2/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true &
wget http://localhost:8001/apify?url=https://datanova.legroupe.laposte.fr/explore/dataset/laposte_hexasmal/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true &
wget http://localhost:8001/apify?url=http://file-examples.com/wp-content/uploads/2017/02/file_example_XLS_10.xls
wget http://localhost:8001/apify?url=http://file-examples.com/wp-content/uploads/2017/02/file_example_XLS_50.xls
wget http://localhost:8001/apify?url=http://file-examples.com/wp-content/uploads/2017/02/file_example_XLS_100.xls
wget http://localhost:8001/apify?url=http://file-examples.com/wp-content/uploads/2017/02/file_example_XLS_1000.xls
wget http://localhost:8001/apify?url=http://file-examples.com/wp-content/uploads/2017/02/file_example_XLS_5000.xls
wait
rm apify\?url\=*


# workers = 1, app.run : 0m31.249s
# workers = 3, app.run : 0m18.498s
# workers = 5, app.run : 0m34.485s
# workers = 10, app.run : 0m40.397s

# workers = 3, hypercorn w = 1 : 0m36.761s
# workers = 3, hypercorn w = 3 : 0m16.607s
# workers = 3, hypercorn w = 5 : 0m34.812s

# workers = 1, hypercorn w = 1 : 0m40.030s
# workers = 1, hypercorn w = 3 : 0m40.920s
# workers = 1, hypercorn w = 5 : 0m42.833s

# workers = 5, hypercorn w = 5 : 0m16.097s

# no shared executor, app.run : 0m35.871s
# no shared executor, hypercorn w = 3 : 0m15.767s
