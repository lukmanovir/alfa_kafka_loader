# alfa_kafka_loader
Загрузчик сырых данных из Apache Kafka в Hadoop.
На вход приложению передаются атрибуты для подключения и загрузки данных из Кафка в Hadoop:
1. targеt table - целевая таблица в Hive, куда будут сохранены данные
2. bootstrap_servers - кластер Кафка
3. kafka_login - логин для подключения
4. kafka_provider_path - путь до hadoop credential container (jceks:/hdfs///user/tech_datalake/jceks/*.jceks)
5. kafka_password_alias - алиас hadoop credential container
6. subscribe_topic - имя топика
7. group_id - группа консьюмера
8. truststore_location - путь до SASL сертификата 
9. truststore_type - тип сертификата
10. partition_attribute - поле по которому выполнять партиционирование, по умолчанию custom (формируется партиция вида - date_part=YYYYMMDD)
11. kafka_security_protocol - протокол
12. kafka_sasl_mechanism - по умолчанию передается PLAIN
13. starting_offsets - по умолчанию передается earliest
