const express = require("express");
const bodyParser = require('body-parser');
const {PythonShell} =require('python-shell');
const shell = require('shelljs');
const axios = require('axios');
const fs = require('fs');

const AWS = require('aws-sdk')
const BUCKET_NAME = 's3-001bucket'
const s3 = new AWS.S3({accessKeyId: 'AKIA6RKMBHZQTUOSXPN5', secretAccessKey: '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'});

const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended : false}));
app.use(express.json());
app.use(express.urlencoded({extended : true}));

const uploadFile = (filepath, filename) => {
  const fileContent = fs.readFileSync(filepath);
  const params = {
    Bucket: BUCKET_NAME,
    Key: filename,
    Body: fileContent
  };
  s3.upload(params, function(err,data){
    if (err) {throw err;}
    console.log(`File uploaded successfully. ${data.Location}`);
  });
};

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
  
  word_token(options_wc, function(message){
    if (message == "Word Count Done"){
      if(shell.exec('sh /word_count/hadoop.sh ' + user_id + ' ' + collection_name + ' ' + start_date + ' ' + end_date).code != 0){
        res.send("Hadoop Failed")
      } else{
        console.log("Hadoop worked");
        var file_name = user_id + collection_name + start_date + end_date + 'wc.csv'

        let options_conv = {
          mode: 'text',
          pythonOptions: ['-u'],
          args: [file_name]
        };

        wc_conv(options_conv, function(message){
          var filepath = '/word_count/result/' + file_name
          uploadFile(filepath, file_name)
	  // res.json({file_name: file_name})
	  res.send(file_name)
        });

      };
    } else {
      res.send("Word Count Python Failed")
    }
  })

});

let word_token = function(options, callback){
  PythonShell.run('/word_count/python_scripts/word_count.py', options, function (err, result){
          if (err) throw err;
          callback(result);
  });
}

let wc_conv = function(options, callback){
  PythonShell.run('/word_count/python_scripts/wc_conv.py', options, function (err, result){
          if (err) throw err;
          callback(result);
  });
}

module.exports = app;
