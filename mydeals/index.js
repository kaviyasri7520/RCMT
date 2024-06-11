const https = require('https')
const request = require('request')
const uri = 'mongodb://localhost:27017/';
const config = require('./config')
const { MongoClient } = require('mongodb');


//const FMTC_API_Deals_Incremental = "https://services.fmtc.co/v2/getDeals?key=40d4c29ebd0211312faa917e1d8b75cc&format=JSON&incremental=1"
const sleep = (milliseconds) => {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}
let client = new MongoClient(config.dblink,
  { useNewUrlParser: true, useUnifiedTopology: true });
const clientPromise = client.connect();

 async function retailMasterCheck() {
   const client = await clientPromise;

   const db = client.db('syenapp_beta');
   let deal_count = 0; var merchantsValue = []; let dealsValue = [];

  console.log('before mongo db');
               var currentDate = new Date().toISOString(); var deletedCount = 0;  var deletedDailyDealsCount=0;
               // var previousDate = new Date(currentDate.setDate(currentDate.getDate() - 1)).toISOString().split('T')[0];
                db.collection('syenapp_fmtc_dealbank').deleteMany({ "dtEndDate": { $exists: true, $ne: "", $lte: currentDate } })
                  .then(resultDeals => {
                     console.log('Deals Deleted count->' + resultDeals.deletedCount);
                    if (resultDeals.deletedCount >= 1) {
                     deletedCount = resultDeals.deletedCount;
                       console.log("Expire deals deleted successfully from syenapp_dealbank collection ->" + deletedCount);

                     } else {
                      console.log("No records found for Expire deals from syenapp_dealbank collection");
                    }
                    console.log(currentDate)
                    db.collection('syenapp_daily_deals').deleteMany({ "dtEndDate": { $exists: true, $ne: "", $lt: currentDate } })
                    .then(daliyDeals => {
                     console.log('Deals Deleted count->' + daliyDeals.deletedCount);
                     if (daliyDeals.deletedCount >= 1) {
                      deletedDailyDealsCount = daliyDeals.deletedCount;
                        console.log("Expire deals deleted successfully from syenapp_dealbank collection ->" + deletedDailyDealsCount);
                     }
                    })
                    let counts=dealCount(db);
                    console.log(counts)
         });

 }

async function dealCount(db) {
  const dealsCount = await db.collection('syenapp_fmtc_dealbank').aggregate([
    { $group: 
      { _id: 
        { 
          merchantId: "$merchantId", 
          merchantName: "$merchantName" 
        }, 
        count: { $sum: 1 } } },
    { 
      $project: 
      { 
        _id: 0, 
        count: 1, 
        merchantId: '$_id.merchantId', 
        merchantName: '$_id.merchantName' } }
  ]).toArray();
  
  const deals = dealsCount.map(doc => {
    return { merchantId: doc.merchantId, merchantName: doc.merchantName, count: doc.count }
  });
 
const bulkUpdateOps = deals.map(deal => ({
  updateOne: {
    filter: { merchantId: deal.merchantId },
    update: { $set: { dealsCount: deal.count } }
  }
}));
let bulkupdate=await db.collection('syenapp_retailmaster').bulkWrite(bulkUpdateOps);

if(bulkupdate.modifiedCount>=0){
  console.log(bulkupdate.modifiedCount)
 
}
else{
  console.log('No documents were modified')
}

 
}

retailMasterCheck();
//  module.exports.handler = async function (event, context) {

//    let firstResult = await retailMasterCheck();
//    await sleep(12000);
//    console.log('firstResult--->' + firstResult);
//    context.done();
//    return firstResult;
//  }
