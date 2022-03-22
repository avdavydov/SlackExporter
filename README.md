# SlackExporter

Скрипт предназначен для экспорта:
1.	Списка каналов Slack
2.	Списка пользователей
3.	Истории сообщений, включая их треды.

## Подготовка скрипта к запуску
1. Создать новое приложение на https://api.slack.com/apps
2. В разделе `OAuth & Permissions` -> `User Token Scopes` добавить следующие `OAuth Scope`:



    * channels:history
    * channels:read
    * groups:history
    * groups:read
    * im:history
    * im:read
    * links:read
    * mpim:history
    * mpim:read
    * reactions:read
    * users.profile:read
    * users:read


3. Выполнить установку приложения в необходимое пространство Slack
4. Выполнить установку приложения в Slack, в разделе `Приложения` 
5. Создать файл token.py и добавить в него строку: 


    token = 'xoxp-****' 


значение для токена нужно взять из `User OAuth Token` раздела `OAuth & Permissions` -> `OAuth Tokens for Your Workspace`

Для установки зависимостей выполнить:
 
 `pip install - r requirements.txt`
