const express = require("express");
const bodyParser = require('body-parser');
const {PythonShell} =require('python-shell');
const shell = require('shelljs');
const axios = require('axios');

const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended : false}));
app.use(express.json());
app.use(express.urlencoded({extended : true}));


app.post("/wordcount", (req,res) => {
  var collection_name = req.body.collection_name;
  var user_id = req.body.user_id;
  var start_date = req.body.start_date;
  var end_date = req.body.end_date;

  let options_wc = {
    mode: 'text',
    pythonOptions: ['-u'],
    args: [collection_name, user_id, start_date, end_date]
  };

  word_count(options_wc, function(message){
    if (message == "Word Count Done"){
      shell.exec('sh hadoop.sh ' + collection_name + user_id + start_date + end_date);
    } else {
      res.send("Word Count Failed")
    }
  })

});

let word_count = function(options, callback){
  PythonShell.run('./python_scripts/word_count.py', options, function (err, result){
          if (err) throw err;
          callback(result);
  });
}

module.exports = app;
