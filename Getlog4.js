/**
 * Created with JetBrains WebStorm.
 * User: jichuntao
 * Date: 14-5-7
 * Time: 下午8:03
 * To change this template use File | Settings | File Templates.
 */
var fs = require('fs');
var lineReader = require('line-reader');
var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('tmp.db');
fs.unlinkSync('tmp.db');
//db.all("CREATE TABLE items(uid varchar(255) primary key, data text);", function(err, ret) {
db.all("CREATE TABLE items(uid varchar(255), data text);", function(err, ret) {
    console.log(err);
    console.log(ret);
    if(err){
        return;
    }
    test();
});

//create table tb_nl(uid varchar(255) primary key, data text);

//return;
//db.serialize(function() {

//    var stmt = db.prepare("INSERT INTO tb_nl VALUES (?,?)");
//    for (var i = 0; i < 10; i++) {
//        stmt.run(['3000'+i,'124']);
//    }
//    stmt.finalize();
//    db.run("INSERT INTO tb_nl VALUES (?,?)",["1231213","adssadasd"]);
//    db.all("SELECT * FROM tb_nl where uid=?",['30001'], function(err, row) {
//        console.log(err);
//        console.log(row);
//    });
//    db.all("select * from sqlite_master where name=?",['tb_nl'], function(err, row) {
//        console.log(err);
//        console.log(row);
//    });
//    db.all("select name from sqlite_master", function(err, row) {
//        console.log(err);
//        console.log(row);
//    });
//});

/**
 * Created with JetBrains WebStorm.
 * User: jichuntao
 * Date: 13-11-4
 * Time: 下午7:04
 * To change this template use File | Settings | File Templates.
 */

var fs = require('fs');
var lineReader = require('line-reader');
var util = require('util');
var zlib = require('zlib');
var query = require("querystring");
var path='/mnt/farmweblog3/preloader2/';
//var path='./xxx/';
var ret={};
var users={};
var tempFile='';
var datedir='2014_5_6';
var langdir='th';
var intervalm;

function test(){
    tempFile=datedir+'_'+langdir+'.tmp';
    fs.writeFileSync(tempFile,'');
    ret={};
    users={};
    var filename=path+datedir+'/'+langdir+'/'+datedir+'_preloader2_'+langdir+'.log';
    openfile(filename,function(data){
        handleAllUser();
        console.log(util.inspect(process.memoryUsage()));
        console.log(ret);
        //fs.writeFileSync('3.log',JSON.stringify(ret),'utf8');
        console.log('Over');
        clearInterval(intervalm)
    });
    intervalm=setInterval(printacc,1000);
}

function exe(req, res, rf, data) {
    var qu = query.parse(data);
    datedir = qu['date'];
    langdir = qu['lang'];
    ret={};
    var filename=path+datedir+'/'+langdir+'/'+datedir+'_netError_'+langdir+'.log';
    openfile(filename,function(data){
        res.write(data, "binary");
        res.end();
    });
}

//开始打开文件
function openfile(filename,callback) {
    lineReader.eachLine(filename, function (line, last, cb) {
        exec(line, function () {
            if (last) {
                var obj={};
                obj.users=users;
                callback(obj);
                cb(false);
                return;
            }
            cb();
        });
    });
}

function exec(strs, cb) {
    try {
        var data = strs.substr(strs.indexOf('{'));
        var key = strs.substring(0, strs.indexOf('{') - 1);
        var obj = JSON.parse(data);
        var logtime = key.split('-')[0];
        var lang = key.split('-')[2];
        var uid = obj.uid;
        var date=new Date(logtime*1000);
        handleItem(uid,obj,cb);
    }
    catch (e){
        console.log('Err:'+e);
        cb();
    }
}
function handleAllUser(){
    ret={};
    var allNum=0;       //所有用户
    var successNum=0;   //成功的总次数
    var failedNum=0;    //失败的总次数
    var successAll=0;   //成功进入用户的数量
    var successFailed=0;   //失败过但是成功进入的用户数量
    var successNew=0;   //新用户成功进入的数量
    var failedAll=0;    //没有进入的用户数量
    var failedNew=0;    //没有进入的新用户数量
    for(var key in users){
        var user=users[key];
        allNum++;
        //处理成功进入失败进入的次数
        var sacc=0;
        var facc=0;
        for(var l in user.ll){
            if(user.ll[l]){
                sacc++;
                successNum++;
            }else{
                facc++;
                failedNum++;
            }
        }

        if(user.isSuccess){
            successAll++;
            if(user.isNew){
                successNew++;
            }
            if(facc>0){
                successFailed++;
            }
        }else{
            failedAll++;
            if(user.isNew){
                failedNew++;
            }
        }
    }
    ret.allNum=allNum;
    ret.successNum=successNum;
    ret.failedNum=failedNum;
    ret.successAll=successAll;
    ret.successFailed=successFailed;
    ret.successNew=successNew;
    ret.failedAll=failedAll;
    ret.failedNew=failedNew;
}
var acc=0;
var acc2=0;
function printacc(){
    console.log(acc+'  q:'+acc2);
    acc2=0;
}
var cache=[];
function handleItem(uid,obj,cb){
    var obj2={};
    var ret={};
    ret.isNew=Boolean(obj.isNew);
    ret.type=obj.type;
    ret.loginSession=obj.loginSession;
    obj2.uid=uid;
    obj2.ret=obj;
    cache.push(obj2);
    if(cache.length>100){
        var stmt = db.prepare("INSERT INTO items VALUES (?,?)");
        for (var i = 0; i < cache.length; i++) {
            var item=cache[i];
            stmt.run([item.uid,JSON.stringify(item.ret)]);
        }
      stmt.finalize();
        cache=[];
    }
    acc++;
    acc2++;
    cb();
}
function handleUser(uid,obj){
    var data;
    if(users[uid]){
        data=users[uid];
    }else{
        data={};
        data.isNew=false;
        data.ll={};
        data.isSuccess=false;
        users[uid]=data;
    }
    if(obj.isNew && !data.isNew){ //是否是新用户
        data.isNew=true;
    }
    pushObjByObj(data,obj.type); //进入 完成 占的比例
    var loginSession=obj.loginSession;//每次的哈希日志
    if(obj.type=='end'){
        data.ll[loginSession]=true;
        data.isSuccess=true;
    }else if(obj.type=='start'){
        if(!data.ll[loginSession]){
            data.ll[loginSession]=false;
        }
    }

}
function pushObjByObj(obj,key){
    if(!obj[key]){
        obj[key]=0;
    }
    obj[key]++;
}

function pushObj(key){
    if(!ret[key]){
        ret[key]=0;
    }
    ret[key]++;
}

exports.exe=exe;