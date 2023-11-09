POSTGRES_JDBC:=jdbc/postgresql-42.6.0.jar
# Format: yyyy-mm-dd
EXECUTE_DATE:=2023-11-08
TABLE:=product

ingest:
	spark-submit --jars ${POSTGRES_JDBC} \
	--conf spark.execute.date=${EXECUTE_DATE} \
	--conf spark.postgres.table=${TABLE} \
	src/spark-ingestion.py

transform:
	spark-submit --conf spark.execute.date=${EXECUTE_DATE} \
	src/spark-transform.py

run-services:
	docker compose up -d

stop-services:
	docker compose down -v