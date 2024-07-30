## Короткий опис рішення. (Саме завдання див. нижче)
**0-fixtures** - створення таблиць та заповнення їх тестовими даними. SQLite база у файлі  **public.sqlite3**

**1-create_duration** - створення view для розрахунку проміжку  часу для кожного статусу статусі. Час для Open беремо із create_at таблиці issues

**2-split_interval** - розбиваємо проміжок часу на коли задачі в статусі In Progress перетинаються в часі для одного виконавця. Визначаємо для кожного проміжку часу які задачі виконувались в робочий час. Рахуємо  оплату за робочий час


Візуалізація рішення для 4 завдання. Шукаємо перетини проміжків часу для задач в статусі In Progress та рахуємо кількість таких перетинів - вага.  
Переоцінюємо проміжок часу для кожної задачі враховуючи 1/Вага.
![Example](example_2.jpg "Example")
![Example](example_1.jpg "Example")





# Завдання
Ваше завдання складається з 5 пунктів, які рекомендовано виконувати послідовно. Результатом пунктів 1 та 2 має бути SQL запит який створить PostgresSQL View.<br>
Починаючи з пункту 3 можна використовувати будь-яку мову програмування(бажано python), або можете використати SQL<br>
Можете створювати будь-яку кількість проміжних таблиць та views<br>

### Структура
##### Таблиця issues:
Таблиця містить в собі всі задачі
- `issue_key` - ключ задачі
- `project_key` - ключ проекту
- `issue_type` - тип задачі (Task, Bug, Improvement, etc)
- `assignee_key` - ключ людини, яка назначена на задачу в даний момент
- `created_at` - дата створення задачі

##### Таблиця changelogs:
Таблиця містить в собі всі зміни статусів задач
- `issue_key` - ключ задачі
- `author_key` - ключ автора, який змінив статус
- `from_status` - з якого статусу задачу перенесли
- `to_status` - в який статус задачу перенесли
- `created_at` - дата зміни статусу

### Завдання:
1. Розрахуйте з якого по який час задача була в певному статусі.<br>
Результат:<br>
View який поверне наступну структуру - `issue_key`, `author_key`, `status`, `start_date`, `end_date`
2. Для кожного запису вирахувати скільки календарного часу в секундах кожна задача була в певному статусі
3. Для кожної задачі над якою працювала людина вирахувати скільки робочого часу задача була в статусі "In progress". Робочий час вважати з 10:00 по 20:00 з понеділка по пятницю
4. Якщо людина працює над декількома задачами одночасно - робочий час за цей період (коли він працює над декількома задачами) ділиться на кількість задач, над якими він працює.

Наприклад,<br>
Jason працював над задачею **A** з 10:00 19.01.2024 до 14:00 23.01.2024<br>
над задачею **B** з 10:00 22.01.2024 до 20:00 23.01.2024<br>
Маємо отримати

| issue_key | author_key | calendar_duration | working_duration |
| --- | --- | --- | --- |
| A | Jason | 360000 | 61200 |
| B | Jason | 122400 | 46800 |

Схематичне відображення:
![Example](example_4.png "Example")
5. Розрахувати загальну вартість всіх задач в межах типу задач. Тип задач знаходиться в таблиці `jira__issues` колонка `issue_type`. Розрахувати за такою логікою:
 - Якщо `author_key` починається з "JIRAUSER" - ціна години часу такого спеціаліста $1.00
 - Якщо `author_key` НЕ починається з "JIRAUSER" - ціна години часу такого спеціаліста $2.00

Результат:<br>
Таблиця з полями - `issue_type`, `total_cost`<br>
Рекомендація:<br>
Для початку розрахуйте вартість кожної задачі. Наступним кроком дістаньте тип для кожної задачі та порахуйте суму для кожного типу
