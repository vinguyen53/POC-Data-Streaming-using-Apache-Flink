create database if not exists flink;
use flink;
create table if not exists transformed_orders (id int, content String) primary key id;