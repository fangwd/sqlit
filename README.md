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


// tokenisation ... (have a look at the template engine like mouthtouch
OrderItems.table.select({ _expr "order.quantity > item.quantity" })

while (true) {
  token = getNextToken()
  if (not token): break
  if token is field:
    push replaced field
  if token is not an operator:
    raise

// may need definition of expressions?
