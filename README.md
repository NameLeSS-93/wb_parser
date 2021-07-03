**REST API для работы с каталогом Wildberries**
----
REST API сервис, который позволяет получать каталог товаров Wildberries и по указанным категориям

отправлять сообщения с информацией об id-товара, названии, цене и размере скидки.

## Запуск приложения
Для запуска нужен установленный [docker-compose](https://docs.docker.com/compose/install/)
```
$ docker-compose up -d --build --scale parser=3
```
Приложение запустится на *0.0.0.0:8080*
## Работа с приложением
Настройи задаются в файле .env


Реализована ручка `POST` /catalog

Пример запроса

```
POST /catalog

{
    "url": "https://www.wildberries.ru/catalog/elektronika/razvlecheniya-i-gadzhety/igrovye-konsoli/playstation"
}
```
Пример ответа
```
200 OK

{
    "success": true,
    "message": "All messages successfully sent to kafka topic wb-category"
}
```
