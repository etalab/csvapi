wget http://localhost:8001/apify?url=http://datanova.legroupe.laposte.fr/explore/dataset/laposte_poincont2/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true &
wget http://localhost:8001/apify?url=https://datanova.legroupe.laposte.fr/explore/dataset/laposte_hexasmal/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true &
wait
rm apify\?url\=*
