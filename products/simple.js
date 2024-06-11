const fs = require('fs');
const ftp = require('ftp');
const zlib = require('zlib');
const path = require('path');
const csv = require('fast-csv');
const { Client } = require('@elastic/elasticsearch');
const url = require('url');
const { MongoClient } = require('mongodb');
const QA = "mongodb://sygnupuser:AsxFBMkGuN3E9r4mQ@10.0.2.237:27017/syenappdb?ssl=false&authSource=admin&retryWrites=true&w=majority"
const prod="mongodb://sygnupprodadmin:BMkGuN3E9r4mTbYhx8y@10.1.3.129:15602,10.1.3.14:65534,10.1.3.183:43065/syenapp_prod?authSource=admin&replicaSet=rs0&w=majority&readPreference=secondary&retryWrites=true&ssl=false"

let client = new MongoClient(QA,
  { useNewUrlParser: true, useUnifiedTopology: true });
const clientPromise = client.connect();

//log method
const appPath = require('path');
const log4js = require("log4js");
const todayDate = new Date();
const todayDateFormat = todayDate.getFullYear() + '-' + (todayDate.getMonth() + 1) + '-' + todayDate.getDate();

log4js.configure({
  appenders: {
      coupon: {
          type: "file",
          filename: appPath.join(__dirname, 'LogFiles/' + todayDateFormat+'_deals' + '.log')
      }
  },
  categories: {
      default: {
          appenders: ["coupon"],
          level: "info"
      }
  }
});

const logger = log4js.getLogger("deals,coupon");

// Elasticsearch configuration
const eClient = new Client({
 node: 'http://10.0.2.154:9200', // Elasticsearch URL
  //  node: 'http://10.1.3.48:9200',//prod
     auth: {
      username: 'sygnupuser',
      password: 'Techminds2023'
     },
    requestTimeout: 60000000 // Increase the timeout value (in milliseconds) as needed
  });

// Excel configuration
const excelFilePath = 'C:\\Users\\tgl256\\Pictures\\ExportProductSubscriptions-12_13_2023.csv';

// FTP configuration
const ftpHost = 'ftp.flexoffers.com';
const ftpUser = 'kaviya@syenappindia.com';
const ftpPassword = 'Kaviya@123';

// Process the Excel file and perform FTP file download/extraction and Elasticsearch indexing
function processExcelFile() {
  const records = [];

  fs.createReadStream(excelFilePath)
    .pipe(csv.parse({ headers: true }))
    .on('data', (data) => {
      records.push(data);
    })
    .on('end', () => {
      processRecords(records);
    })
    .on('error', (err) => {
      console.error('Error reading Excel file:', err);
    });
}

// Process the records by performing FTP file download/extraction and Elasticsearch indexing
async function processRecords(records) {
  const totalRecords = records.length;
  let processedCount = 0;
  const client = await clientPromise;

  const db = client.db('syenapp_beta');
 

  for (const record of records) {
    const { Id, ProgramName, CatalogID, CatalogName, Url, Country, Flag} = record;

    const merchants = await db.collection('syenapp_retailmaster').aggregate([
      { '$match': { 'merchantname': ProgramName } },
      {
        '$project': {
          'merchantname': true,
          'merchantId': true,
          'domain':true,
          'domainurl':true,
          'country': true,
          'shipsToCountries': true,
          '_id': false
        }
      }
    ]).toArray();


        const merchantsDetails = merchants.map(doc =>{ 
      return { merchantid:doc.merchantId,merchantName:doc.merchantname,domain:doc.domain,domainurl:doc.domainurl}
    });   
    let  merchantNAME='';
    let domainname= '';
    let merchantID='';


if(merchantsDetails.length>0){ 
 merchantNAME=merchantsDetails.length === 0 ? '' + merchantsDetails[0].merchantName : merchantsDetails[0].merchantName
    domainname= merchantsDetails.length === 0 ? '' + merchantsDetails[0].domain : merchantsDetails[0].domain
    merchantID=merchantsDetails.length === 0 ? '' + merchantsDetails[0].merchantid : merchantsDetails[0].merchantid
}

    const localFilePath = `C:\\Users\\tgl256\\Downloads\\${merchantID}_${ProgramName}.csv.gz`;
    const extractedFolderPath = `C:\\Users\\tgl256\\Downloads\\${merchantID}_${ProgramName}`;
    const extractedFilePath = path.join(extractedFolderPath, `${merchantID}_${ProgramName}.csv`);
    const remoteFilePath = url.parse(Url).pathname;

    const client = new ftp();

    await new Promise((resolve, reject) => {
      client.connect({
        host: ftpHost,
        user: ftpUser,
        password: ftpPassword
      });

      client.on('ready', () => {
        processRow(client, remoteFilePath, localFilePath, extractedFolderPath, extractedFilePath, Country, ProgramName, Flag,Id,merchantNAME,domainname,merchantID)
          .then(() => {
            processedCount++;
            console.log(`Processed record ${processedCount} of ${totalRecords}`);

            if (processedCount === totalRecords) {
              console.log('Processing complete');
              client.end();
            }

            resolve();
          })
          .catch((err) => {
            console.error(`Error processing record ${processedCount + 1}:`, err);
            processedCount++;

            if (processedCount === totalRecords) {
              console.log('Processing complete');
              client.end();
            }

            reject(err);
          });
      });

      client.on('error', (err) => {
        reject(`Error connecting to FTP server: ${err}`);
      });
    });
  }
}

// Process a row by downloading, extracting, and indexing the file
function processRow(client, remoteFilePath, localFilePath, extractedFolderPath, extractedFilePath, country, ProgramName, Flag,Id,merchantNAME,domainname,merchantID) {
  return new Promise((resolve, reject) => {
    const csvFilePaths = [];

    if (Flag === '0') {
      client.get(remoteFilePath, (err, stream) => {
        if (err) {
            resolve(`Error retrieving file from FTP server: ${err}`);
          return;
        }

        const fileStream = fs.createWriteStream(localFilePath);
        stream.pipe(fileStream);

        fileStream.on('finish', () => {
          console.log('File downloaded successfully');

          try {
            fs.mkdirSync(extractedFolderPath, { recursive: true });
          } catch (err) {
            resolve(`Error creating extracted folder: ${err}`);
            return;
          }

          const extractStream = fs.createReadStream(localFilePath)
            .pipe(zlib.createGunzip())
            .pipe(fs.createWriteStream(extractedFilePath));

          extractStream.on('finish', () => {
            console.log('File extracted successfully');

            fs.readdir(extractedFolderPath, (err, files) => {
              if (err) {
                resolve(`Error reading extracted folder: ${err}`);
                return;
              }

              files.forEach((file) => {
                if (file.endsWith('.csv')) {
                  const csvFilePath = path.join(extractedFolderPath, file);
                  csvFilePaths.push(csvFilePath);
                }
              });

              console.log('CSV file paths:', csvFilePaths);

              indexRecords(csvFilePaths, country, ProgramName,Id,merchantNAME,domainname,merchantID)
                .then(() => {
                  resolve();
                })
                .catch((err) => {
                    resolve(`Error indexing records: ${err}`);
                });
            });
          });

          extractStream.on('error', (err) => {
            resolve(`Error extracting file: ${err}`);
          });
        });

        fileStream.on('error', (err) => {
            resolve(`Error saving file: ${err}`);
        });
      });
    } else {
      // Flag is 1, skip FTP download and extract
      console.log('Skipping FTP download and extraction for ProgramName:', ProgramName);
      indexRecords(csvFilePaths, country, ProgramName,Id,merchantNAME,domainname,merchantID)
        .then(() => {
          resolve();
        })
        .catch((err) => {
            resolve(`Error indexing records: ${err}`);
        });
    }
  });
}

// Index the CSV records into Elasticsearch
function indexRecords(csvFilePaths, country, ProgramName, Id,merchantNAME,domainname,merchantID) {
    const promises = [];
  
    for (const csvFilePath of csvFilePaths) {
      promises.push(
        new Promise((resolve, reject) => {
          const records = [];
  
          fs.createReadStream(csvFilePath)
            .pipe(csv.parse({ headers: true }))
            .on('data', (data) => {
              records.push(data);
            })
            .on('end', () => {
              // Filter duplicates based on the 'description' field
              const uniqueRecords = filterDuplicates(records, 'Description');
  
              bulkIndex(uniqueRecords, country, ProgramName, Id,merchantNAME,domainname,merchantID)
                .then(() => {
                  console.log(`Successfully indexed records from CSV: ${csvFilePath}`);
                  resolve();
                })
                .catch((err) => {
                  console.log(err)
                  resolve(`Error indexing records from CSV: ${csvFilePath}: ${err}`);
                });
            })
            .on('error', (err) => {
              resolve(`Error reading CSV file: ${csvFilePath}: ${err}`);
            });
        })
      );
    }
  
    return Promise.all(promises);
  }
  
  // Filter duplicates based on a specific field
  function filterDuplicates(records, field) {
    const uniqueRecords = [];
    const uniqueValues = new Set();
  
    for (const record of records) {
      const value = record[field];
      if (!uniqueValues.has(value)) {
        uniqueValues.add(value);
        uniqueRecords.push(record);
      }
    }
  
    return uniqueRecords;
  }
  
  

// Perform bulk indexing for a batch of records
async function bulkIndex(records, country, ProgramName,Id,merchantNAME,domainname,merchantID) {
  const body = [];

  // // Check if merchantName already exists
  // const existingMerchantResponse = await eClient.search({
  //   index: 'syenapp_fmtc_products',
  //   body: {
  //     query: {
  //       match: {
  //         merchantID: parseInt(Id)
  //       }
  //     }
  //   }
  // });

    // const deleteResponse = await eClient.deleteByQuery({
    //   index: 'syenapp_fmtc_products',
    //   body: {
    //     query: {
    //       match: {
    //         merchantID: parseInt(Id)
    //       }
    //     }
    //   }
    // });

    // console.log(`Deleted ${deleteResponse.deleted} documents with merchantName: ${merchantID}`);
  

  for (const record of records) {
    const {
      Name,
      Description,
      Category,
      CategoryId,
      FinalPrice,
      ImageURL,
      PriceCurrency,
      LinkUrl
    } = record;
    const salePrice = parseFloat(FinalPrice);
    if (salePrice >= 0) {
    const document = {
      merchantID: merchantID,
      productName: Name,
      description: Description,
      categories: Category,
      domain:domainname,
      categoryId: CategoryId,
      salePrice: salePrice,
      productImage: ImageURL,
      currency: PriceCurrency,
      affiliateUrl: LinkUrl,
      country: country.toLowerCase(),
      createdDate: new Date(),
      merchantName: merchantNAME,
      network: 'FO'
    };
    body.push({ index: { _index: 'syenapp_fmtc_products' } });
    body.push(document);
  }

   
  }

  return eClient.bulk({ refresh: true, body });
}

// Start the processing
processExcelFile();
