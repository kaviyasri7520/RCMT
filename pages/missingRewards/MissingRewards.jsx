import React,{useEffect, useState, useMemo} from 'react'
import axios from 'axios'
import './MissingRewards.css'
import DataTable from "react-data-table-component";
import Select from 'react-select';
import FilterComponent from "../../components/filter/FilterComponents";
import Modal from "../../components/popUpModals/modal";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import HideTestUsers from "../../components/hideTestUsers/HideTestUsers";
import { apiBaseURL } from '../../core/utils';

const { apiStoreHeader, apiOcrHeader, apiElasticHeader, testUsersURL } = require('../../config/config.json')

const MissingRewards = () => {
    const [userTaskTrackingList,setUserTaskTrackingList] = useState([])
    const [totalList,setTotalList] = useState([])
    const [filterText, setFilterText] = React.useState("");
    const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
    const [pending, setPending] = React.useState(true);

    const [transactionAmount, setTransactionAmount] = useState('');
    const [rewardPoints,setRewardPoints] = useState('')

    const [dynamicRewardPoints,setDynamicRewardPoints] = useState({})
    const [showApprove,setShowApprove] = useState(false)
    const [showReject, setShowReject] = useState(false);
    const [rejectReason, setRejectReason] = useState("");
    const [rowDetails,setRowDetails] = useState({})
    const [currentCountry] = useState(localStorage.getItem("selectedCountry"))
    const [submitTrack,setSubmitTrack] = useState(false)
    const [expandableRowShow,setExpandableRowShow] = useState(false)
    const [merchantList,setMerchantList] = useState([])
    const [merchantName,setMerchantName] = useState("")
    const [tempMerchantName,setTempMerchantName] = useState("")
    const [selectedFilter,setSelectedFilter] = useState("all")

    const [testUsersList,setTestUsersList] =  useState([])
    
    

    useEffect(() => {
      axios.get(`https://${apiBaseURL()}/v2/rewards/getconfigrewardpoints`)
      .then((res) => {
        const result = res.data.data[0]
        setDynamicRewardPoints(result)
      })
      .catch((err) => {console.log(err);alert('Cannot fetch the reward points')})  

      axios({
          url:`https://${apiBaseURL()}/v2/rcmtcontroller/usertasktrackinglist`,
          method:"get",
          headers:apiStoreHeader
      }).then((res) => {
        const result = res.data.data;
        // Sort records by status first (pending records come first)
        result.sort((a, b) => {
            if (a.status === 'pending' && b.status !== 'pending') return -1;
            if (a.status !== 'pending' && b.status === 'pending') return 1;
            // If both records have the same status, sort by createdDate
            return new Date(b.createdDate) - new Date(a.createdDate);
        });
        setUserTaskTrackingList(result);
        setTotalList(result); // Update totalList as well
        setSelectedFilter('all');
        setPending(false);

      })
      .catch((err) => {
          console.log(err)
      })

      axios({
        url:`https://${apiBaseURL()}/v2/rcmtcontroller/fetchretailerinfo`,
        method:'get',
        headers:apiStoreHeader
      }).then((res) => {
        console.log(res)
        const result = res.data.data
        if(result !== undefined && result !== null && Array.isArray(result)){
          let options = result.map(e => ({label:e.merchantname,value:e.merchantname}))
          setMerchantList(options)
        }   
      })
      .catch((err) => console.log(err))

            // get list of test users
            axios.get(`${testUsersURL}/testusers-list`)
            .then((res) => {
              let result = res.data
              setTestUsersList(result)
            })
            .catch((err) => {console.log(err)})
            document.getElementById("hidetestusers").checked = false ;
    },[submitTrack])

    

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
            fontSize: "16px",
            fontWeight:"800",
            fontFamily:"poppins",
            backgroundColor: " #333222",
            minHeight: "52px",
            borderBottomWidth: "1px",
            borderBottomStyle: "solid",
          },
          denseStyle: {
            minHeight: "32px",
          },
        },
        rows: {
          style: {
            minHeight: "52px", // override the row height
            fontSize: "13px",
            fontFamily:"poppins",
            textAlign: "center",
            color: "black",
          },
        },
        headCells: {
          style: {
            color: "#F5F6F6",
            textDecoration: "none",
            fontSize: "14px",
            textAlign: "center",
            justifyContent: "center",
          },
        },
        pagination: {
          style: {
            color: "black",
            fontSize: "13px",
            minHeight: "56px",
            fontWeight: "bolder",
            backgroundColor: "white",
            borderTopStyle: "solid",
            borderTopWidth: "1px",
            borderTopColor: "#3F6EAE",
          },
          pageButtonsStyle: {
            borderRadius: "50%",
            height: "40px",
            width: "40px",
            padding: "8px",
            cursor: "pointer",
            transition: "0.4s",
            color: "white",
            fill: "black",
            backgroundColor: "transparent",
            "&:hover:not(:disabled)": {
              backgroundColor: "#3b7acc",
              fill: "black",
            },
            "&:focus": {
              outline: "none",
              backgroundColor: "#13243A",
              fill: "white",
            },
          },
        },
      }

      const columns = [
        {
          name: <div>Referral Code</div>,
          selector: (row) => row.syenapprefcode,
          center: "true",
        },
        {
          name: <div>Tasks</div>,
          selector: (row) => row.flag,
          center: "true",
        },
        {
          name: <div>Verified</div>,
          selector: (row) => row.isVerified,
          center: "true",
        },
        {
          name: <div>Status</div>,
          selector: (row) => row.status,
          center: "true",
          sortable:true,
        },
        {
          name: <div>Created Date</div>,
          selector: (row) => row.createdDate ? new Date(row.createdDate).toString().substr(4,12) : '',
          center: "true",
          sortable: true,
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
              {row.status !== "approved" &&
                row.status !== "redeemed" &&
                row.status !== "rejected" && (
                  <button
                    className="view-address-btn"
                    onClick={() => handleApprove(row)}
                  >
                    Approve
                  </button>
                )}
            </div>
          ),
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
              {row.status !== "approved" &&
                row.status !== "redeemed" &&
                row.status !== "rejected" && (
                  <button
                    className="reward-Reject-btn"
                    onClick={() => handleReject(row)}
                  >
                    Reject
                  </button>
                )}
            </div>
          ),
          center: "true",
        },
      ];

    //approve pop up
    const handleApprove = (row) => {
      setRowDetails(row)
      setShowApprove(true)
      setTempMerchantName(row.merchantName)
    }

    // reject pop up
    const handleReject = (row) => {
        setRowDetails(row);
        setShowReject(true);
    };

    const hidePopup = () => {
      setShowApprove(false)
      setShowReject(false);
      setMerchantName("")
    };

    //approve
    const submitApproved = () => {
     console.log(rowDetails)
    const params1 = {
      mailId: rowDetails.syenappid,
      flag: rowDetails.flag,
      status: "APPROVED",
      id: rowDetails._id
    }
    if(rowDetails.flag !== "MISSINGREWARDS" || (merchantName !== "" && transactionAmount !== "" && rewardPoints !== "")){
      axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/updateusertasktracking`,params1,
      {
        headers:apiStoreHeader
      })
      .then((res) => {
        const result = res.data
        if(result.status === 1){
          setSubmitTrack(!submitTrack)
          setShowApprove(false)
        }
      })
      .catch((err) => {
        console.log(err)
        toast.error('failed to update')
      })
      const params2 = {
        mailId: rowDetails.syenappid,
        flag: rowDetails.flag,
        rewardPoints: parseInt(dynamicRewardPoints.firstInAppPurchase) ,
        status: rowDetails.status,
      };
      const elasticParams = {
        params: {
          syenappId: rowDetails.syenappid,
          key: rowDetails.flag,
          rewardPoints: parseInt(dynamicRewardPoints.firstInAppPurchase) ,
        },
      };
      if(rowDetails.flag === "FIRSTINAPPPURCHASE"){
        axios.put(`https://${apiBaseURL()}/v2/rewards/updatetaskrewards`, params2, {
          headers: apiStoreHeader,
        }).then((res) => {
          const result = res.data
          console.log(result)
          
            // update the record in elastic search
          axios.put(`https://${apiBaseURL()}/elasearch/addusertask`, elasticParams, {
              headers: apiElasticHeader,
          })
          .then((res) => {
            console.log(res);
            result.status === 1 && toast.success(`${rowDetails.syenappid} first purchase reward submitted`, { autoClose: 1500 })
          })
          .catch((error) => {
            console.log(error);
            toast.error("failed update the record in elastic search");
          });

        if (result.status === 1) {
          setShowApprove(false);
          axios({
              url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${rowDetails.syenappid}&flag=InAppPurchase`,
              method: "get",
              headers: apiOcrHeader,
          })
          .then((res) => {
            console.log(res)
            })
            .catch((err) => {
              console.log(err);
              toast.error('notification failed to sent')
            });
          }
        }).catch((err) => {
          console.log(err)
          toast.error('failed to update')
        })    
      }

    if(rowDetails.flag === "MISSINGREWARDS"){
        const rewards2xParams = {
          mailId:  rowDetails.syenapprefcode,
          rewardPoints:parseInt(Math.round(transactionAmount) * parseInt(rewardPoints)),
          status: "claimed",
          merchantName: rowDetails.merchantName,
          // returnDays: merchantDetails.returnDays,
          // messageId: transactionDetails.messageId,
          transAmount: Math.round(transactionAmount)
        };
          //to update category(x) rewards
        axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/update2xrewards`,rewards2xParams)
        .then((res) => {
          const result1 = res.data;
            if (result1.status === 1) {
              setMerchantName("")
              toast.success(`${rowDetails.syenapprefcode} missing reward submitted`, { autoClose: 1500 });
              setShowApprove(false);
              setTransactionAmount("")
              setRewardPoints("")
              axios({
                url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${rowDetails.syenappid}&flag=MISSINGREWARDS`,
                method: "get",
                headers: apiOcrHeader,
              }).then(() => console.log('2x reward notification sent') ).catch((err) => toast.error('Missing rewards notification failed to sent'))
              }
          })
          .catch((err) => {
          console.log(err);
          toast.error("failed to update");
    });       
    }
  }
  if(rowDetails.flag==="MISSINGREWARDS"){
    if(merchantName === ""){
      toast.error("Please select the Merchant name", { autoClose: 1800 })
    }
    if(transactionAmount === ""){
        toast.error("Please enter the transaction amount", { autoClose: 1800 })
    }
    if(rewardPoints === ""){
        toast.error("Please enter the category points", { autoClose: 1800 })
    }
  }
  }
    
  // reject 
    const submitReject = () => {
      const params = {
        mailId: rowDetails.syenappid,
        flag: rowDetails.flag,
        status: "REJECTED", 
        rejectReason:rejectReason,
        id: rowDetails._id
        }
    
        if(rejectReason.trim() !== ""){
          axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/updateusertasktracking`,params,
          {
            headers:apiStoreHeader
          })
          .then((res) => {
            const result = res.data
            if(result.status === 1){
              setSubmitTrack(!submitTrack)
              setShowReject(false)
              axios({
                url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${rowDetails.syenappid}&flag=MISSINGREWARDSREJECT`,
                method: "get",
                headers: apiOcrHeader,
              }).then(() => console.log('Rejected Missing Rewards notification sent') ).catch((err) => toast.error('Rejected Missing Rewards notification failed to sent'))
            }
          })
          .catch((err) => {
            console.log(err)
            toast.error('failed to update')
          })
        }
        else{
          toast.error('Please enter reason for reject')
        }
    }

    //hide pop up
    const filterChange=({target}) => {
      let value = target.value
      setSelectedFilter(value)
        if(value === "FIRSTINAPPPURCHASE"){
          let FirstInAppPurchase = totalList.filter((item) => item.flag === "FIRSTINAPPPURCHASE")
          setUserTaskTrackingList(FirstInAppPurchase)
        }
        if(value === "MISSINGREWARDS"){
          let MissingRewards = totalList.filter((item) => item.flag === "MISSINGREWARDS")
          setUserTaskTrackingList(MissingRewards)
        }
        if(value === "all"){
          setUserTaskTrackingList(totalList)
        }
    }

    const func_handleInputs = ({ target }) => {
      if(target.name === 'transactionAmount'){
        setTransactionAmount(target.value.replace(/[a-zA-Z]/g, "").replace( /[^\w.]|_/g,""));
      }

     if(target.name === 'RPmutipleby' && target.value <= 100){
      setRewardPoints(target.value.replace(/[a-zA-Z]/g, "").replace(/[^\w]|_/gi, ""))
     }
     if(target.name === 'RPmutipleby' && target.value > 100){
      if(target.value > 100){
        toast.warn('Category points is limited to 100',{ autoClose: 1300 })
      }
     }
    };

    const func_handleMerchantName = (event) => {
      setMerchantName(event.value)
      setRowDetails(prev => ({...prev,merchantName:event.value}))
    }

    const filteredItems = userTaskTrackingList.filter(
        (item) =>
          JSON.stringify(item).toLowerCase().indexOf(filterText.toLowerCase()) !==
          -1
    );

  const func_hideshowTestUser = () => {
    let currentState = document.getElementById("hidetestusers").checked;
    console.log(currentState)
    if(currentState){
      let testUsersRemovedData = userTaskTrackingList.filter(({syenappId:id1}) => (!testUsersList.some(({syenappId:id2}) => id2 === id1)))
      setUserTaskTrackingList(testUsersRemovedData)
    }
    if(!currentState){
      setUserTaskTrackingList(totalList)
    }
  }

  const subHeaderComponent = (
    <HideTestUsers key="hide-test-users-component" handleChange={func_hideshowTestUser} checkboxId={'hidetestusers'} />
  )


    const subHeaderComponent1 = useMemo(() => {
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

      const approveConfirmation_popUp = (
        <Modal show={showApprove} handleClose={hidePopup}>
          <h2 className="title">Do you want to Continue?</h2>
          <div className="action-container">
            {rowDetails.flag === "MISSINGREWARDS" && <>
            <label className='merchant-select-label'>Merchant Name (From user)</label>
            <input
              className="reject-input"
              type="text"
              value={tempMerchantName}
              placeholder="Total reward points"
              disabled={true}
            />
            <label className='merchant-select-label'>Merchant Name</label>
            <Select className='merchant-select' options={merchantList} onChange={func_handleMerchantName} value={{label:merchantName,value:merchantName}} placeholder="Merchant name" />
            <input
              className="reject-input"
              type="text"
              name="transactionAmount"
              onChange={func_handleInputs}
              value={transactionAmount}
              placeholder="Transaction amount"
              autoComplete="off"
            />
               <input
              className="reject-input"
              type="text"
              name="RPmutipleby"
              onChange={func_handleInputs}
              value={rewardPoints}
              placeholder="Category points"
              autoComplete="off"
            />
              <input
              className="reject-input"
              type="text"
              value={Math.round(transactionAmount)*parseInt(rewardPoints)|| ""}
              placeholder="Total reward points"
              disabled={true}
            /></>}
            <div className="VIA-button-container">
              <button onClick={() => {setShowApprove(false);setMerchantName("")}} className="confirm">
                Cancel
              </button>
              <button onClick={submitApproved} className="cancel">
                Yes, approve
              </button>
            </div>
          </div>
        </Modal>
      );

      const rejectConfirmation_popUp = (
        <Modal show={showReject} handleClose={hidePopup}>
          <h2 className="title">Do you want to Continue?</h2>
          <div className="action-container">
            <input
              className="reject-input"
              type="text"
              onChange={(e) => setRejectReason(e.target.value)}
              value={rejectReason}
              placeholder="Mention the Reason"
              autoComplete="off"
            />
            <div className="VIA-button-container">
              <button onClick={() => {setShowReject(false);setMerchantName("")}} className="confirm">
                Cancel
              </button>
              <button onClick={submitReject} className="cancel">
                Yes, reject
              </button>
            </div>
          </div>
        </Modal>
      );

      const ExpandedComponent = ({ data }) => (
        <table className='missing-rewards-extended-table'>
          <tbody>
            <tr className='bs-extended-table-tr'>
              <th colSpan={5} className="thead">Additional Details</th>
            </tr>
            <tr className='bs-extended-table-tr'>
              <td >Merchant name </td>
              <td >{data.merchantName}</td>
            </tr>
            <tr className='bs-extended-table-tr'>
            <td >Purchase date </td>
            <td >{data.purchaseDate}</td>
            </tr>
            <tr className='bs-extended-table-tr'>
              <td>SyenApp account used </td>
              <td>{data.isSyenAppIdUsed}</td>
            </tr>
          </tbody>
        </table>
      );

    return(
        <div>
            <select onChange={filterChange} value={selectedFilter}  >
                <option value="all">All</option>
              
                {currentCountry === "in" && (
                <option value="FIRSTINAPPPURCHASE">First in app purchase</option>
            )}
                <option value="MISSINGREWARDS">Missing rewards</option>
            </select>
            <DataTable
            columns={columns}
            data={filteredItems}
            noDataComponent={"No records found"}
            customStyles={customStyles}
            progressPending={pending}
            pagination
            paginationRowsPerPageOptions={[25, 50, 75, 100]}
            paginationPerPage={25}
            subHeader
            subHeaderComponent={[subHeaderComponent,subHeaderComponent1]}
            expandableRows
            expandableRowDisabled ={(row) => row.flag === "FIRSTINAPPPURCHASE"}
            expandableRowsComponent={ExpandedComponent}
            onChangePage={() => setExpandableRowShow(false)}
            expandableRowExpanded={() => expandableRowShow}
          />
        {approveConfirmation_popUp}
        {rejectConfirmation_popUp}
        <ToastContainer toastClassName={"custom-toast"} />
        </div>
    )
}
export default MissingRewards