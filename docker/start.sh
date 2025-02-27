#!/bin/bash

set -x

DB="${DB:-cassandra}"
ENABLE_ES="${ENABLE_ES:-false}"
ES_PORT="${ES_PORT:-9200}"
ES_VERSION="${ES_VERSION:-v6}"
RF=${RF:-1}

# cassandra env
export CASSANDRA_USER="${CASSANDRA_USER:-cassandra}"
export CASSANDRA_PASSWORD="${CASSANDRA_PASSWORD:-cassandra}"
export KEYSPACE="${KEYSPACE:-cadence}"
export VISIBILITY_KEYSPACE="${VISIBILITY_KEYSPACE:-cadence_visibility}"
export CASSANDRA_PROTO_VERSION="${CASSANDRA_PROTO_VERSION:-4}"

# mysql env
export DBNAME="${DBNAME:-cadence}"
export VISIBILITY_DBNAME="${VISIBILITY_DBNAME:-cadence_visibility}"
export DB_PORT=${DB_PORT:-3306}

# elasticsearch env
export VISIBILITY_NAME="${VISIBILITY_NAME:-cadence-visibility-dev}"

setup_cassandra_schema() {
    SCHEMA_DIR=$CADENCE_HOME/schema/cassandra/cadence/versioned
    cadence-cassandra-tool --ep $CASSANDRA_SEEDS create -k $KEYSPACE --rf $RF
    cadence-cassandra-tool --ep $CASSANDRA_SEEDS -k $KEYSPACE setup-schema -v 0.0
    cadence-cassandra-tool --ep $CASSANDRA_SEEDS -k $KEYSPACE update-schema -d $SCHEMA_DIR
    VISIBILITY_SCHEMA_DIR=$CADENCE_HOME/schema/cassandra/visibility/versioned
    cadence-cassandra-tool --ep $CASSANDRA_SEEDS create -k $VISIBILITY_KEYSPACE --rf $RF
    cadence-cassandra-tool --ep $CASSANDRA_SEEDS -k $VISIBILITY_KEYSPACE setup-schema -v 0.0
    cadence-cassandra-tool --ep $CASSANDRA_SEEDS -k $VISIBILITY_KEYSPACE update-schema -d $VISIBILITY_SCHEMA_DIR
    echo "Registering domain for Cassandra..."
    cqlsh $CASSANDRA_SEEDS -k $KEYSPACE -f /etc/cadence/domain/cassandra.cql
}

setup_mysql_schema() {
    SCHEMA_DIR=$CADENCE_HOME/schema/mysql/v8/cadence/versioned
    if [ "$MYSQL_TX_ISOLATION_COMPAT" == "true" ]; then
        CONNECT_ATTR='--connect-attributes tx_isolation=READ-COMMITTED'
    fi
    cadence-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD $CONNECT_ATTR create --db $DBNAME
    cadence-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD $CONNECT_ATTR --db $DBNAME setup-schema -v 0.0
    cadence-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD $CONNECT_ATTR --db $DBNAME update-schema -d $SCHEMA_DIR
    VISIBILITY_SCHEMA_DIR=$CADENCE_HOME/schema/mysql/v8/visibility/versioned
    cadence-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD $CONNECT_ATTR create --db $VISIBILITY_DBNAME
    cadence-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD --db $VISIBILITY_DBNAME $CONNECT_ATTR setup-schema -v 0.0
    cadence-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD --db $VISIBILITY_DBNAME $CONNECT_ATTR update-schema -d $VISIBILITY_SCHEMA_DIR
    echo "Registering domain for MySQL..."
    mysql -h $MYSQL_HOST -u $MYSQL_USER -p $MYSQL_PASSWORD $MYSQL_DATABASE < /etc/cadence/domain/mysql.sql
}

setup_postgres_schema() {
    SCHEMA_DIR=$CADENCE_HOME/schema/postgres/cadence/versioned
    cadence-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw "$POSTGRES_PWD" -p $DB_PORT create --db $DBNAME
    cadence-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw "$POSTGRES_PWD" -p $DB_PORT --db $DBNAME setup-schema -v 0.0
    cadence-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw "$POSTGRES_PWD" -p $DB_PORT --db $DBNAME update-schema -d $SCHEMA_DIR
    VISIBILITY_SCHEMA_DIR=$CADENCE_HOME/schema/postgres/visibility/versioned
    cadence-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw "$POSTGRES_PWD" -p $DB_PORT create --db $VISIBILITY_DBNAME
    cadence-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw "$POSTGRES_PWD" -p $DB_PORT --db $VISIBILITY_DBNAME setup-schema -v 0.0
    cadence-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw "$POSTGRES_PWD" -p $DB_PORT --db $VISIBILITY_DBNAME update-schema -d $VISIBILITY_SCHEMA_DIR
    echo "Registering domain for PostgreSQL..."
    PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DATABASE -f /etc/cadence/domain/postgres.sql
}


setup_es_template() {
    SCHEMA_FILE=$CADENCE_HOME/schema/elasticsearch/$ES_VERSION/visibility/index_template.json
    server=`echo $ES_SEEDS | awk -F ',' '{print $1}'`
    URL="http://$server:$ES_PORT/_template/cadence-visibility-template"
    curl -X PUT $URL -H 'Content-Type: application/json' --data-binary "@$SCHEMA_FILE"
    URL="http://$server:$ES_PORT/$VISIBILITY_NAME"
    curl -X PUT $URL
}

setup_schema() {
    if [ "$DB" == "mysql" ]; then
        echo 'setup mysql schema'
        setup_mysql_schema
    elif [ "$DB" == "postgres" ]; then
        echo 'setup postgres schema'
        setup_postgres_schema
    else
        echo 'setup cassandra schema'
        setup_cassandra_schema
    fi

    if [ "$ENABLE_ES" == "true" ]; then
        setup_es_template
    fi
}

wait_for_cassandra() {
    server=`echo $CASSANDRA_SEEDS | awk -F ',' '{print $1}'`
    until cqlsh -u $CASSANDRA_USER -p $CASSANDRA_PASSWORD --cqlversion=3.4.6 --protocol-version=$CASSANDRA_PROTO_VERSION $server < /dev/null; do
        echo 'waiting for cassandra to start up'
        sleep 1
    done
    echo 'cassandra started'
}

wait_for_scylla() {
    server=`echo $CASSANDRA_SEEDS | awk -F ',' '{print $1}'`
    until cqlsh -u $CASSANDRA_USER -p $CASSANDRA_PASSWORD --cqlversion=3.3.1 --protocol-version=$CASSANDRA_PROTO_VERSION $server < /dev/null; do
        echo 'waiting for scylla to start up'
        sleep 1
    done
    echo 'scylla started'
}

wait_for_mysql() {
    server=`echo $MYSQL_SEEDS | awk -F ',' '{print $1}'`
    nc -z $server $DB_PORT < /dev/null
    until [ $? -eq 0 ]; do
        echo 'waiting for mysql to start up'
        sleep 1
        nc -z $server $DB_PORT < /dev/null
    done
    echo 'mysql started'
}

wait_for_postgres() {
    server=`echo $POSTGRES_SEEDS | awk -F ',' '{print $1}'`
    nc -z $server $DB_PORT < /dev/null
    until [ $? -eq 0 ]; do
        echo 'waiting for postgres to start up'
        sleep 1
        nc -z $server $DB_PORT < /dev/null
    done
    echo 'postgres started'
}


wait_for_es() {
    server=`echo $ES_SEEDS | awk -F ',' '{print $1}'`
    URL="http://$server:$ES_PORT"
    curl -s $URL > /dev/null 2>&1
    until [ $? -eq 0 ]; do
        echo 'waiting for elasticsearch to start up'
        sleep 1
        curl -s $URL > /dev/null 2>&1
    done
    echo 'elasticsearch started'
}

wait_for_db() {
    if [ "$DB" == "mysql" ]; then
        wait_for_mysql
    elif [ "$DB" == "postgres" ]; then
        wait_for_postgres
    elif [ "$DB" == "scylla" ]; then
        wait_for_scylla
    else
        wait_for_cassandra
    fi

    if [ "$ENABLE_ES" == "true" ]; then
        wait_for_es
    fi
}

wait_for_async_wf_queue_kafka() {
    ready="false"
    while [ "$ready" != "true" ]; do
        brokers=$(echo dump | nc "$ZOOKEEPER_SEEDS" "$ZOOKEEPER_PORT" | grep brokers | wc -l)
        if [ "$brokers" -gt 0 ]; then
            ready="true"
        else
            echo 'waiting for kafka broker to show up in zookeeper'
            sleep 3
        fi
    done

    echo 'kafka broker started'
}

setup_async_wf_queue() {
    if [ "$ASYNC_WF_KAFKA_QUEUE_ENABLED" != "true" ]; then
        return
    fi

    wait_for_async_wf_queue_kafka

    # For now we are using wurstmeister/kafka image which has a default topic auto creation via KAFKA_CREATE_TOPICS environment variable
    # So no additional setup needed here.
}

wait_for_db
if [ "$SKIP_SCHEMA_SETUP" != true ]; then
    setup_schema
fi

setup_async_wf_queue

exec /start-cadence.sh
