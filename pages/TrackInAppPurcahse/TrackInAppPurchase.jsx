import React, { useState, useMemo } from "react";
import DataTable from "react-data-table-component";
import PopUpModal from "../../components/popUpModals/PopUpModal";
import "./TrackInAppPurchase.css";
import axios from "axios";
import BtnLoadingSpinner from "../../components/loadingSpinner/BtnLoadingSpinner";
import FilterComponent from "../../components/filter/FilterComponents";
import { FiMail } from "react-icons/fi";
import HideTestUsers from "../../components/hideTestUsers/HideTestUsers";
import { apiBaseURL } from "../../core/utils";

var base64 = require("base-64");
var decode = require("urldecode");
const {
  testUsersURL,
  apiOcrHeader,
  apiElasticHeader,
  mailContentViewAuthorizedUser,
} = require("../../config/config.json");

const TrackInAppPurchase = () => {
  const [filterText, setFilterText] = useState("");
  const [resetPaginationToggle, setResetPaginationToggle] = useState(false);

  const [userData, setUserData] = useState([]);
  const [usersViewData, setUsersViewData] = useState([]);
  const [syenappId, setSyenappId] = useState("");
  const [merchantName, setMerchantName] = useState("");
  const [lastDate, setLastDate] = useState("");
  const [syenappIdList, setsyenappIdList] = useState([]);
  const [show, setShow] = useState(false);
  const [pending, setPending] = useState(true);
  const [processing, setProcessing] = useState(false);
  const [mailContent, setMailContent] = useState("");
  const [showMail, setShowMail] = useState(false);

  const [selectedMessageId, setSelectedMessageId] = useState("");
  const [loading, setloading] = useState(false);
  const [verifiedAuthentication, setVerifiedAuthentication] = useState(false);
  const [isRewarded, setIsRewarded] = useState(false);
  const [submitTrack, setSubmitTrack] = useState(false);

  const [transactionAmount, setTransactionAmount] = useState(null);

  const [transactionDetails, setTransactiondetails] = useState({});

  const [dynamicRewardPoints,setDynamicRewardPoints] = useState({})
  const [minPurchaseAmount,setMinPurchaseAmount] = useState('')
  const [merchantDetails,setMerchantDetails] = useState({})
  const [expandableRowShow,setExpandableRowShow] = useState(false)

  const [testUsersList,setTestUsersList] =  useState([])
  const [totalUsersList,setTotalUsersList] = useState([])

  const [orderId,setOrderId] = useState('')

  React.useEffect(() => {
    axios.get(`https://${apiBaseURL()}/v2/rewards/getconfigrewardpoints`)
    .then((res) => {
      const result = res.data.data[0]
      setMinPurchaseAmount(result.minPurchaseAmt.replace('$',''))
      setDynamicRewardPoints(result)
    })
    .catch((err) => {console.log(err);alert('Cannot fetch the reward points')})

    const currentUser = atob(localStorage.getItem("userId"));
    const userVerification =
      mailContentViewAuthorizedUser.includes(currentUser);

    setVerifiedAuthentication(userVerification);
  }, []);

  React.useEffect(() => {
    axios({
      url: `https://${apiBaseURL()}/textsearch/trackpurchaseusers`,
      method: "get",
      headers: apiElasticHeader,
    })
    .then((res) => {
        const data = res.data.data;
        setUserData(data || []);
        setTotalUsersList(data || [])
        setPending(false)
      })
      .catch((err) => {
        console.log(err);
        alert("connection failed");
      });
      // get list of test users
      axios.get(`${testUsersURL}/testusers-list`)
      .then((res) => {
        let result = res.data
        setTestUsersList(result)
      })
      .catch((err) => {console.log(err)})
      document.getElementById("hidetestusers").checked = false ;
  }, [submitTrack]);

  const customStyles = {
    noData: {
      style: {
        marginTop: "16%",
        color: "#3b7acc",
        fontSize: "30px",
      },
    },
    headRow: {
      style: {
        color: "white",
        fontSize: "15px",
        backgroundColor: "#333222",
        minHeight: "52px",
        borderBottomWidth: "1px",
        borderBottomStyle: "solid",
      },
      denseStyle: {
        minHeight: "32px",
      },
    },
  };

  const columns = [
    {
      name: <div>SyenApp Id</div>,
      selector: (row) => row.syenappId,
      sortable: true,
      center: "true",
    },
    {
      name: <div>Merchant Name</div>,
      selector: (row) => row.merchantName,
      center: "true",
    },
    {
      name: <div>Subject</div>,
      selector: (row) => row.subject,
      center: "true",
    },
    {
      name: <div>Last Date</div>,
      selector: (row) => row.lastDate ? row.lastDate.substr(0, 10): '',
      sortable: true,
      center: "true",
    },
    {
      name: <div>Is Reward Released</div>,
      selector: (row) => row.isInAppPurReleased.toString(),
      center: "true",
    },
    {
      name: <div>Reward Released Date</div>,
      selector: (row) => row.inAppPurReleaseDate ? row.inAppPurReleaseDate.substr(0, 10):' ',
      center: "true",
    },
    {
      name: <div></div>,
      button: true,
      cell: (row) => (
        <div
          className="action-btns"
          style={{
            fontSize: "11px ",
            fontFamily: "sans-serif",
            color: "black",
          }}
        >
          <button className="view-address-btn" onClick={() => func_viewTrackURLs(row)}>
            Track URL's
          </button>
        </div>
      ),
      center: "true",
    },
    verifiedAuthentication
      ? {
          name: <div></div>,
          button: true,
          cell: (row) => (
            <div
              className="action-btns"
              style={{
                fontSize: "11px ",
                fontFamily: "sans-serif",
                color: "black",
              }}
            >
              {row.messageId === selectedMessageId && loading ? (
                <button className="view-mail-btn" disabled>
                  <BtnLoadingSpinner />{" "}
                </button>
              ) : (
                <button
                  className="view-mail-btn"
                  onClick={() => openMailContent(row)}
                >
                  View Mail
                  <FiMail className="mail-icon" />
                </button>
              )}
            </div>
          ),
          center: "true",
        }
      : {
          name: <div></div>,
          button: true,
          center: "true",
        },
  ];

  const ExpandedComponent = ({ data }) => (
    <table>
      <tbody>
        <tr>
          <th className="expanded-subject-head">Message Id</th>
          <th className="expanded-subject-head">Subject</th>
        </tr>
        <tr>
          <td className="expanded-subject">{data.messageId}</td>
          <td className="expanded-subject">{data.subject}</td>
        </tr>
      </tbody>
    </table>
  );

  // open mail content
  const openMailContent = ({ messageId }) => {
    setSelectedMessageId(messageId);
    // console.log(messageId)
    setloading(true);
    axios
      .put(`https://${apiBaseURL()}/v2/sygapp/getmailcontent`, { id: messageId })
      .then((res) => {
        const result = res.data[0] || {};
        var res1 = base64.decode(result.mail_content) || result.mail_content;
        setMailContent(res1);
        setloading(false);
        setShowMail(true);
      })
      .catch((err) => {
        console.log(err);
        alert("request failed");
        setloading(false);
      });
  };

  axios
      .get(`https://${apiBaseURL()}/v2/rcmtcontroller/syenappidList`)
      .then((res) => {
        const result = res.data;
        let idList = result.map((e) => e.syenapprefcode);
        setsyenappIdList(result);
     
      })
      .catch((error) => {
        console.log(error);
         alert("connection failed");
      });

      const getSyenapprefcodeBySyenappid = (syenappId) => {
        // Find the object with the given syenappid in the list
        const foundItem = syenappIdList.find(item => item.syenappid === syenappId);
        console.log(foundItem)
        // If found, return the syenapprefcode, otherwise return null or handle the case as needed
        return foundItem ? foundItem.syenapprefcode : null;
      }

  // open pop up
  const func_viewTrackURLs = (row) => {
    // console.log(row)
    setTransactionAmount('')
    setOrderId('')
    setTransactiondetails(row);
    const params = {
      mailId: row.syenappId,
      mName: row.merchantName,
      lastDate: row.lastDate,
    };
    axios.put(`https://${apiBaseURL()}/elasearch/getuserpurchasedetails`, params, {
        headers: apiElasticHeader,
      })
      .then((res) => {
        const data = res.data;
        setUsersViewData(data.data);
      })
      .catch((err) => { console.log(err); alert("request failed")});

      axios.get(`https://${apiBaseURL()}/v2/rcmtcontroller/getmerchantdetails?retailerId=${row.retailId}`)
      .then((res) => {
        const result = res.data.data;
        console.log(result)
        setMerchantDetails(result)
      })
      .catch((err) => {
        console.log(err);alert(err)
      })
    setSyenappId(row.syenappId);
    setMerchantName(row.merchantName);
    setIsRewarded(row.isInAppPurReleased);
    setLastDate(new Date(row.lastDate).toString().substr(4, 12));
    setShow(true);
    setProcessing(false);
  };
  // close pop up
  const closePop_up = () => {
    setUsersViewData([]);
    setShow(false);
    setTransactionAmount(null)
    setOrderId(null)
  };
  const closeMail = () => {
    setShowMail(false);
  };

  const submit = () => {
    if (dynamicRewardPoints.firstInAppPurchase !== '' &&  transactionAmount.length > 0 && Math.round(transactionAmount) !== 0 && merchantDetails.rewardPoints !== '' && parseInt(merchantDetails.rewardPoints) >= 1 ) {
      setProcessing(true);
      const syenapprefcode = getSyenapprefcodeBySyenappid(syenappId);

      const Params = {
        mailId:syenappId,
        merchantName:merchantName,
        transAmount:Math.round(transactionAmount),
        transDate:transactionDetails.lastDate,
        returnDays:merchantDetails.returnDays,
        orderId:orderId
      };
      const data = {
        mailId: syenappId,
        merchantName: merchantName,
      };

      // const elasticParams = {
      //   params: {
      //     syenappId: syenappId,
      //     key: "FIRSTINAPPPURCHASE",
      //     rewardPoints: parseInt(dynamicRewardPoints.firstInAppPurchase) ,
      //   },
      // };
      if(Math.round(transactionAmount) >= parseInt(minPurchaseAmount ) && transactionAmount >= 20){
        if(orderId.trim() !== ''){
          axios.put(`https://${apiBaseURL()}/v2/rewards/addeligiblepurchasehistory`,Params)
          .then((res) => {
            const  result = res.data
            if(result.status === 1){console.log('eligible purchase history added')} 
            // console.log(Params)
            setOrderId('')
          })
          .catch((err) => {
            console.log(err)
          })


           axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/updatedreferedrewards`, data, {
         // headers: apiStoreHeader,
        })
        .then((res) => {
          const data = res.data;
          console.log(data)
          setSubmitTrack(!submitTrack);
       
         
          
          if(data.status === -1  || data.status === 0){
          const rewards2xParams = {
            mailId: syenapprefcode,
            rewardPoints:parseInt(Math.round(transactionAmount) * parseInt(merchantDetails.rewardPoints)),
            status: "claimed",
            merchantName: transactionDetails.merchantName,
            returnDays: merchantDetails.returnDays,
            messageId: transactionDetails.messageId,
            transAmount: Math.round(transactionAmount),
          };
          console.log(rewards2xParams)
            //to update 2x rewards
          axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/update2xrewards`,rewards2xParams)
          .then((res) => {
              const result1 = res.data;
              console.log(result1)
              if (result1.status === 1) {
                alert("In-app purchase reward submitted");
                // axios({
                //   url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=2XREWARDS`,
                //   method: "get",
                //   headers: apiOcrHeader,
                // }).then(() => console.log('2x reward notification sent') ).catch((err) => console.log('2x reward notification failed to sent'))
                setProcessing(false);
                setShow(false);
                axios({
                  url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=2XREWARDS`,
                  method: "get",
                  headers: apiOcrHeader,
                }).then(() => console.log('2x reward notification sent') ).catch((err) => console.log('2x reward notification failed to sent'))
              }
          })
          .catch((err) => {
          console.log(err);
          alert("request failed");
          setProcessing(false);
          });

    // update user referral elgigible
      axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/updateuserreferraleligible`,  { mailId: syenappId } )
      .then((res) => {
        const result = res.data;
          result.status === 1 && !isRewarded && alert("user referral enabled");
          setTransactionAmount(null);
          setProcessing(false);
          setShow(false);
      })
      .catch((err) => {
        alert("failed to enable user referral");
        setProcessing(false);
        setTransactionAmount(null);
        setShow(false);
      });
         }
         else{
          alert("Submitted successfully");
          axios({
            url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=REFERFRIEND`,
            method: "get",
            headers: apiOcrHeader,
          }).then(() => console.log('2x reward notification sent') ).catch((err) => console.log('2x reward notification failed to sent'))
          setProcessing(false);
         }
        
          })
        // //update in mongodb
        // axios.put(`https://${apiBaseURL()}/v2/rewards/updatetaskrewards`, Params, {
        //   headers: apiStoreHeader,
        // })
        // .then((res) => {
          // const data = res.data;
          // setSubmitTrack(!submitTrack);
          // update the record in elastic search
          // axios.put(`https://${apiBaseURL()}/elasearch/addusertask`, elasticParams, {
          //     headers: apiElasticHeader,
          //   })
          //   .then((res) => {
          //     setShow(false);
          //     console.log(res);
          //   })
          //   .catch((error) => {
          //     console.log(error);
          //     alert("failed update the record in elastic search");
          //   });

          // if (data.status === 1 && !isRewarded) {
          //   axios({
          //     url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=InAppPurchase`,
          //     method: "get",
          //     headers: apiOcrHeader,
          //   })
          //   .then((res) => {
          //     if (res.data.data === 1) {
          //         setShow(false);
          //         setProcessing(false);
          //         !isRewarded && alert("submitted successfully");
          //       }
          //     })
          //     .catch((err) => {
          //       console.log(err);
          //       setProcessing(false);
          //     });
          // }
          //  else {
          //   !isRewarded && alert("submitted successfully");
          //   setProcessing(false);
          // }
      
          // // get the merchant details
        //       const rewards2xParams = {
        //         mailId: transactionDetails.syenappId,
        //         rewardPoints:parseInt(Math.round(transactionAmount) * parseInt(merchantDetails.rewardPoints)),
        //         status: "claimed",
        //         merchantName: transactionDetails.merchantName,
        //         returnDays: merchantDetails.returnDays,
        //         messageId: transactionDetails.messageId,
        //         transAmount: Math.round(transactionAmount),
        //       };
        //       console.log(rewards2xParams)
        //         //to update 2x rewards
        //       axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/update2xrewards`,rewards2xParams)
        //       .then((res) => {
        //           const result1 = res.data;
        //           if (result1.status === 1) {
        //             alert("In-app purchase reward submitted");
        //             setProcessing(false);
        //             setShow(false);
        //             axios({
        //               url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=2XREWARDS`,
        //               method: "get",
        //               headers: apiOcrHeader,
        //             }).then(() => console.log('2x reward notification sent') ).catch((err) => console.log('2x reward notification failed to sent'))
        //           }
        //       })
        //       .catch((err) => {
        //       console.log(err);
        //       alert("request failed");
        //       setProcessing(false);
        //       });

        // // update user referral elgigible
        //   axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/updateuserreferraleligible`,  { mailId: syenappId } )
        //   .then((res) => {
        //     const result = res.data;
        //       result.status === 1 && !isRewarded && alert("user referral enabled");
        //       setTransactionAmount(null);
        //       setProcessing(false);
        //       setShow(false);
        //   })
        //   .catch((err) => {
        //     alert("failed to enable user referral");
        //     setProcessing(false);
        //     setTransactionAmount(null);
        //     setShow(false);
        //   });
              
      // })
      // .catch((err) => {
      //   console.log(err);
      //   alert("submit failed");
      //   setProcessing(false);
      // });
    }
    else{
      alert('Please enter the Order ID')
      setProcessing(false)
    }
      } else{
        console.log(transactionDetails.retailId)
          //get the merchant details
                const rewards2xParams = {
                  mailId: syenapprefcode,
                  rewardPoints: parseInt(Math.round(transactionAmount) * parseInt(merchantDetails.rewardPoints)),
                  status: "claimed",
                  merchantName: transactionDetails.merchantName,
                  returnDays: merchantDetails.returnDays,
                  messageId: transactionDetails.messageId,
                  transAmount: Math.round(transactionAmount),
                };
                console.log('second',rewards2xParams)
                  //to update category(x) rewards
                axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/update2xrewards`,rewards2xParams)
                .then((res) => {
                    const result1 = res.data;
                    if (result1.status === 1) {
                      alert("In-app purchase reward submitted");
                      setProcessing(false);
                      setSubmitTrack(!submitTrack)
                      setShow(false);
                      axios({
                        url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=2XREWARDS`,
                        method: "get",
                        headers: apiOcrHeader,
                      }).then(() => console.log('2x reward notification sent') ).catch((err) => console.log('2x reward notification failed to sent'))
                    }
                })
                .catch((err) => {
                console.log(err);
                alert("request failed");
                setProcessing(false);
          });
          // // unused code
      // if(Math.round(transactionAmount) >= parseInt(minPurchaseAmount)) {
      //           axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/updateuserreferraleligible`,  { mailId: syenappId } )
      //           .then((res) => {
      //             const result = res.data;
      //               result.status === 1 && !isRewarded && alert("user referral enabled");
      //               setTransactionAmount(null);
      //               setProcessing(false);
      //               setShow(false);
      //       })
      //           .catch((err) => {
      //             alert("failed to enable user referral");
      //             setProcessing(false);
      //             setTransactionAmount(null);
      //             setShow(false);
      //           });
      //         }
      }
   
    } else {
      if (transactionAmount.length === 0) {
        alert("Please enter the Transaction amount");
      }
      if (Math.round(transactionAmount) === 0) {
        alert("Transaction amount should not be 0");
      }
      if(dynamicRewardPoints.firstInAppPurchase === ('' || undefined)){
        alert('Cannot fetch the reward points')
      }
      if(merchantDetails.rewardPoints === '' && merchantDetails.rewardPoints < 1){
        alert('"Reward Points multiply by" should not empty or 0')
      }
    }
  };

  // table header
  const headers = [
    {
      header0: "Created Date",
      header1: "C Source",
      header2: "Current URL",
      header3: "Parsed Current URL",
      header4: "Previous URL",
      header5: "Description",
      header6: "Qty Sent Out",
      header7: "Edit",
      header: "Delete",
    }
  ];
  const tableHeader = headers.map((c, index) => {
    return (
      <tr key={index} className="InAppPurchase-tablerow-head">
        <th className="mi-tableView-title">{c.header0}</th>
        <th className="mi-tableView-title">{c.header1}</th>
        <th className="mi-tableView-title">{c.header2}</th>
        <th className="mi-tableView-title">{c.header3}</th>
        <th className="mi-tableView-title">{c.header4}</th>
        <th className="mi-tableView-title">{c.header5}</th>
      </tr>
    );
  });

  const func_handleInputs = ({ target }) => {
    if(target.name === 'transactionAmount'){
      setTransactionAmount(target.value.replace(/[a-zA-Z]/g, "").replace( /[^\w.]|_/g,""));
    }
   if(target.name === 'RPmutipleby'){
    setMerchantDetails(prev => ({...prev,rewardPoints:target.value.replace(/[a-zA-Z]/g, "").replace(/[^\w\s]|_/gi, "")}))
   }
   if(target.name === 'orderId'){
    setOrderId(target.value)
   }
  };

  const info_box = (
    <div className="purchase-info-header">
        <div className="purchase-info-row" >
          <div className="purchase-info-cells">syenapp Id: {syenappId}</div>
          <div className="purchase-info-cells">Merchant Name: {merchantName}</div>
          <div className="purchase-info-cells">Last date: {new Date(lastDate).toString().substr(0, 10)}</div>
          <div className="purchase-info-cells">
        <label>Order Id: </label>
            <input
              name="orderId"
              value={orderId}
              onChange={func_handleInputs}
              autoComplete="off"
            />
          </div>
        </div>
        <div className="purchase-info-row">
        <div className="purchase-info-cells">
        <label>Transaction Amount: </label>
            <input
              name="transactionAmount"
              value={transactionAmount}
              onChange={func_handleInputs}
              autoComplete="off"
            />
          </div>
          <div className="purchase-info-cells">
           <label>Category Points: </label>
            <input
              name="RPmutipleby"
              value={merchantDetails.rewardPoints}
              onChange={func_handleInputs}
              autoComplete="off"
            />
          </div>
    
          <div className="purchase-info-cells" >Total Reward Points: <input className="total-points-view" value={Math.round(transactionAmount)*parseInt(merchantDetails.rewardPoints)} disabled="true"/></div>
          <div className="purchase-info-cells" ><button className="track-purchase-submit" onClick={submit}>
              {processing ? <BtnLoadingSpinner /> : "Submit"}
            </button></div>  
        </div>
    </div>
  );

  const popUpView = (
    <PopUpModal show={show} handleClose={closePop_up}>
      {info_box}
      <div style={{ padding: "7px" }}></div>
      <div className="track-urls-table">
        <table>
          <thead>{tableHeader}</thead>
          <tbody>
            {Array.isArray(usersViewData) &&
              usersViewData.map((data, index) => {
                return (
                  <tr key={index} className="InAppPurchase-tablerow-data">
                    <td>{new Date(data.cDate).toString().substr(4, 12)}</td>
                    <td>{data.cSource}</td>
                    <td
                      style={{
                        color: "#0e60bd",
                        fontWeight: "bold",
                        letterSpacing: "1px",
                      }}
                    >
                      {data.cURL}
                    </td>
                    <td style={{ fontWeight: "normal", letterSpacing: "2px" }}>
                      {decode(data.cURL)}
                    </td>
                    <td
                      style={{
                        color: "#0e60bd",
                        fontWeight: "bold",
                        letterSpacing: "1px",
                      }}
                    >
                      {data.pURL}
                    </td>
                    <td>{data.desc}</td>
                  </tr>
                );
              })}
          </tbody>
        </table>
      </div>
    </PopUpModal>
  );
  const mailContentView = (
    <PopUpModal show={showMail} handleClose={closeMail}>
      <div className="mail-view-block" dangerouslySetInnerHTML={{ __html: mailContent }}></div>
    </PopUpModal>
  );

  //filter
  const filteredItems = userData.filter(
    (item) =>
      JSON.stringify(item).toLowerCase().indexOf(filterText.toLowerCase()) !==
      -1
  );

  const func_hideshowTestUser = () => {
    let currentState = document.getElementById("hidetestusers").checked;
    console.log(currentState)
    if(currentState){
      let testUsersRemovedData = userData.filter(({syenappId:id1}) => (!testUsersList.some(({syenappId:id2}) => id2 === id1)))
      setUserData(testUsersRemovedData)
    }
    if(!currentState){
      setUserData(totalUsersList)
    }
  }

  const subHeaderComponent1 = (
    <HideTestUsers key="hide-test-users-component" handleChange={func_hideshowTestUser} checkboxId={'hidetestusers'} />
  )

  //filter component inpput
  const subHeaderComponent2 = useMemo(() => {
    const handleClear = () => {
      if (filterText) {
        setResetPaginationToggle(!resetPaginationToggle);
        setFilterText("");
      }
    };

    return (
      <FilterComponent
        key="filter-component"
        onFilter={(e) => setFilterText(e.target.value)}
        onClear={handleClear}
        filterText={filterText}
      />
    );
  }, [filterText, resetPaginationToggle]);

  return (
    <div>
      <DataTable
        columns={columns}
        customStyles={customStyles}
        data={filteredItems}
        subHeader
        subHeaderComponent={[subHeaderComponent1,subHeaderComponent2]}
        progressPending={pending}
        pagination
        paginationRowsPerPageOptions={[25, 50, 75, 100]}
        paginationPerPage={25}
        paginationComponentOptions={{
          rowsPerPageText: "Records per page:",
          rangeSeparatorText: "out of",
        }}
        expandableRows
        expandableRowsComponent={ExpandedComponent}
        onChangePage={() => setExpandableRowShow(false)}
        expandableRowExpanded={() => expandableRowShow}
      />
      {popUpView}
      {mailContentView}
    </div>
  );
};

export default TrackInAppPurchase;