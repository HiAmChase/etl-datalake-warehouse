create table if not exists "user" (
	id integer primary key,
	username varchar,
	last_name varchar,
	first_name varchar,
	email varchar
);

create table if not exists "product" (
	id integer primary key,
	name varchar,
	category varchar,
	description varchar,
	price numeric(10, 2)
);

create table if not exists "order" (
	id integer primary key,
	customer_id integer,
	order_date date,
	CONSTRAINT fk_customer_id FOREIGN KEY(customer_id) REFERENCES "user"(id)
);

create table if not exists "order-detail" (
	id integer primary key,
	order_id integer,
	product_id integer,
	quantity integer,
	CONSTRAINT fk_order_id FOREIGN KEY(order_id) REFERENCES "order"(id),
	constraint fk_product_id foreign key (product_id) references "product"(id)
);
