# UniCon

## Universal Connector v3

Умеет ходить в разные источники вытягивать оттуда данные и сливать в NxLog
Работает с очередью RabbitMQ и хранилищем Redis

Примеры конфигов в папке ./appconfig

Пример готового docker-compose.yml, кторый можно использовать в бою
```dockerfile
version: '3.5'
services:
  unicon:
    image: unicon:v3
    environment:
      - APP_CONFIG_PATH=/appconfig
    networks:
      - internal
    ports:
      - 9400:5000
    restart: on-failure
    depends_on:
      - rabbitmq
      - redis
    volumes:
      - "/opt/unicon_v3/appconfig:/appconfig"

  redis:
    image: 'docker.io/bitnami/redis:6.0-debian-10'
    environment:
      - REDIS_PASSWORD=pass
#      - ALLOW_EMPTY_PASSWORD=yes
#      - REDIS_DISABLE_COMMANDS=FLUSHDB,FLUSHALL
    hostname: redis
    networks:
      - internal
    ports:
      - '6379:6379'
    volumes:
      - 'redis_data:/bitnami/redis/data'

  rabbitmq:
    image: 'rabbitmq:3-management'
    environment:
      - RABBITMQ_DEFAULT_USER=user
      - RABBITMQ_DEFAULT_PASS=pass
    networks:
      - internal
    ports:
      - '5672:5672'
      - '15672:15672'
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:15672"]
      interval: 30s
      timeout: 10s
      retries: 5
    volumes:
      - 'rabbitmq_data:/var/lib/rabbitmq'

networks:
  internal:

volumes:
  redis_data:
    driver: local
  rabbitmq_data:
    driver: local
```
