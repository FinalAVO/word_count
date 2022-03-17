const express = require("express");
const bodyParser = require('body-parser');

const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended : false}));
app.use(express.json());
app.use(express.urlencoded({extended : true}));


app.get("/analysis/report", (req,res) => {
  res.send("Hello, this is report part!")
});



module.exports = app;
