# Installation

`$ npm install datalink`

# Usage

TBA

By default, all columns in a table will be fetched. If you don't need to select
some columns, you can do so by using a structure like

```js
User.table.select({
  password: false;
})
```

When the value of the above is a string, it will be used as the alias of the column:

```js
User.table.select({
  mobile: 'phone';
})
```

Tables are joined transparently when a foreign key field is expanded:

```js
OrderItem.table.select({
  order: { user: { password: false } }
});
```
