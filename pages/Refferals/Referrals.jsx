import React, { useEffect, useState, useMemo } from "react";
import FilterComponent from "../../components/filter/FilterComponents";
import axios from 'axios';

import Modal from '../../components/popUpModals/modal';
import './Refferals.css';
import BtnLoadingSpinner from "../../components/loadingSpinner/BtnLoadingSpinner";
import HideTestUsers from "../../components/hideTestUsers/HideTestUsers";
import { apiBaseURL } from "../../core/utils";
import { FaSync } from 'react-icons/fa';
import DataTable, { createTheme } from 'react-data-table-component';
import { blue, grey } from "@material-ui/core/colors";

const { apiStoreHeader, apiOcrHeader, testUsersURL } = require('../../config/config.json')




const Refferal = () => {
    const [pending,setPending] = useState(false)
    const [users, setUsers] = useState([]);
    const [show, setShow] = useState(false);
    const [approve,setApprove]=useState('');
    const [update, setUpdate] = useState(0);
    const [referralSyenAppId, setRefferalid] = useState('');
    const [referredSyenAppId,setReferredSyenAppId]=useState('');
    const [flag,setFlag]=useState('');
    const [rewardPoints,setRewards]=useState('');
    const [reason, setReason] = useState("");
    const [filterText, setFilterText] = useState("");
    const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
    const [expendableRowShow,setExpandableRowShow] = useState(false)
    const [currentCountry] = useState(localStorage.getItem("selectedCountry"))
    const [testUsersList,setTestUsersList] =  useState([])
    const [totalDetails,setTotalDetails] = useState([])
    const [approvalStatus, setApprovalStatus] = useState({}); // New state to track approval status
    
    const handleRefresh = () => {
      // Reload the current page
      window.location.reload();
    };
    const refreshButton = (
      <button onClick={handleRefresh} className="refresh-button" style={{ color: "white", borderRadius: "10px", border: "0px solid white", background:  '#42a5f5', width: "90px", height: "28px", borderStyle: "none", cursor: "pointer" }}>
        <FaSync />
        Refresh
      </button>
    );
    

    useEffect(() => {
        setPending(true)
        axios({
            //url: `https://${apiBaseURL()}/v2/rcmtcontroller/referraluserlist`,
            url: `https://${apiBaseURL()}/v2/rcmtcontroller/getuserslist`,
            method: 'get',
            headers: apiStoreHeader
        })
        .then((res) => {
            setPending(false)
            const data = res.data.filter(row => !row.isRCMTVerified);
            console.log(data);
                setTotalDetails(data)
                setUsers(data)
            }).catch((error) => { console.log(error);alert('connection failed') })
    // get list of test users
        axios.get(`${testUsersURL}/testusers-list`)
        .then((res) => {
        let result = res.data
        setTestUsersList(result)
        })
        .catch((err) => {console.log(err)})
        document.getElementById("hidetestusers").checked = false ;
    }, [update])

      
    const handleReject = (row) => {
        setRefferalid(row.syenappid);
        setReferredSyenAppId(row.referredBy);
        showrejPopup();
        setApprove('Reject');
      };

    const handleApprove=(referralSyenAppId,referredSyenAppId, flag,  rewardPoints, referralKumulosToken, referredKumulosToken) => {
        setRefferalid(referralSyenAppId);
        setReferredSyenAppId(referredSyenAppId)
        setFlag(flag);
        setRewards(rewardPoints);
        showapprovalsPopup(referralSyenAppId);
        setApprove('Approved')
    }


    const onReject = () => {     
        const param = {
            syenappid:referralSyenAppId,
            referredBy:referredSyenAppId,
            rejectReason: reason,
        }
        console.log(param)
        axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/rejectUser`,param
            )
            .then((res) => {
                console.log("rejectapi")
                setUpdate(update + 1);
            
                if (res) {
                    alert("Submitted Successfully");
                    hiderejPopup();
                    setApprovalStatus((prevStatus) => ({
                        ...prevStatus,
                        [referralSyenAppId]: 'rejected',
                      }));
                }

            }).catch((error) => { console.log(error);alert('request failed') })
    }



    const onApprove = (row) => {
      
        console.log("approveee started")
        const syenappID=row.syenappid
        const param = {
            syenappid: syenappID,
            country:currentCountry,
           referredBy: row.referredBy,
        }
        //console.log(param)
        const param1 = {
            syenappid: syenappID,       
        }
        //console.log(param1)
        axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/approveisrcmtflag`, param1,
        {headers: apiStoreHeader}

    ).then((data) => {
   if(data.status==200 && row.referredBy != undefined){
    // axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/approveuserverfication`, param,
    //     {headers: apiStoreHeader}

    // ).then((res) => {
    //   let referree=res.data.referredBySyenappid
    //   console.log(res.data)
    //   const Approve = res.status;
    
    //   console.log(referree)
    //   setUsers(prevUsers => prevUsers.filter(user => user.syenappid !== referralSyenAppId));

        
    //       setApprovalStatus((prevStatus) => ({
    //         ...prevStatus,
    //         [referralSyenAppId]: 'approved',
    //       }));
    //       setUpdate(update + 1);
        alert("Submitted Successfully");
    //     console.log(referredSyenAppId)
    // // notification for referralSyenAppId
    // axios.get(`https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${referree}&flag=ApproveReferralUser`,
    // {headers: apiOcrHeader
    // }).then((res) => {console.log(res)
    //     }).catch((error) => { console.log(error) })
   
    // // notification for referredSyenAppId
    // axios.get(`https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappID}&flag=ApproveReferredUser`,
    // {headers: apiOcrHeader
    // }).then((res) => {console.log(res)}).catch((err) => console.log(err))    
    // }).catch((error) => { console.log(error);
    
   
    //   hideapprovalsPopup()
    // // Set the approval status for the current row to 'approved'
    // setApprovalStatus((prevStatus) => ({
    //   ...prevStatus,
    //   [row.syenappid]: 'approved',
    // }));

    // // // notification for referralSyenAppId
    // // axios.get(`https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${row.syenappid}&flag=ApproveReferralUser`,
    // // {headers: apiOcrHeader
    // // }).then((res) => {console.log(res)
    // //     }).catch((error) => { console.log(error) })

    // // // notification for referredSyenAppId
    // // axios.get(`https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${referredSyenAppId}&flag=ApproveReferredUser`,
    // // {headers: apiOcrHeader
    // // }).then((res) => {console.log(res)}).catch((err) => console.log(err))    
    // // }).catch((error) => { console.log(error);
    //   alert('request failed') })
  }
  else{
    alert("Submitted Successfully");
  }
})
    }

    const showapprovalsPopup = (referralSyenAppId) => {
        console.log(referralSyenAppId)
        setShow(true);
    }

    const hideapprovalsPopup = () => {
        setShow(false);
    }

    const showrejPopup = (syenappid) => {
        console.log(syenappid)
        setShow(true);
    }
    const hiderejPopup = () => {
        setShow(false);
    }
    const onchanges = (event) => {
        setReason(event.target.value);
      };

      const rejectionpopup = (
        <Modal show={show} handleClose={hiderejPopup}>
          <div className="empty"></div>
          <h2 className="title">Do you want to Reject?</h2>
          <input
            value={reason}
            onChange={onchanges}
            type="tel"
            placeholder="Enter Reason"
            style={{ width: "350px", height: "40px" }}
          ></input>
          <button onClick={hiderejPopup} className="confirm">Cancel</button>
          <button onClick={onReject} className="cancel">Confirm</button>
        </Modal>
      );

      const approvalpopup = (
        <Modal show={show} handleClose={hideapprovalsPopup}>
          <div className="empty"></div>
          <h2 className="title">Do you want to Approve ?</h2>
          <button onClick={hideapprovalsPopup} className="confirm">Cancel</button>
          <button onClick={() => onApprove({ syenappid: referralSyenAppId })} className="cancel">
            Confirm
          </button>
        </Modal>
      );


    const customStyles = {

        noData: {
           style: {
                marginTop: '16%',
                color: '#3b7acc',
                fontSize: '30px',
            },
        },
        headRow: {
            style: {
                color:'white',
                fontSize:'15px',
                backgroundColor: ' #333222',
                minHeight: '52px',
                borderBottomWidth: '1px',
                borderBottomStyle: 'solid',
            },
            denseStyle: {
                  minHeight: '32px',
            },
        },
    }
    const formatDate = (dateString) => {
        try {
          const date = new Date(dateString);
          if (isNaN(date.getTime())) {
            return 'Invalid Date';
          }
          return date.toISOString().split('T')[0];
        } catch (error) {
          console.error('Error formatting date:', error);
          return 'Invalid Date';
        }
      };
    const columns = [
        {
            name: <div>SyenappId</div>,
            selector: row => row.syenappid,
            sortable:true,
            center: "true"
        },
        {
            name: <div>Phone Number</div>,
            selector: row =>row.phone,
            sortable:true,
            center: "true"
        },
        {
            name: <div  >Referral Code</div>,
            selector: row =>row.referralCode,
            sortable:true,
            center: "true"
        },
        {
            name: <div>Referred By</div>,
            selector: row =>row.referredBy,
            center: "true"
        },
        {
            name: <div>User Created Date</div>,
            selector: row => formatDate(row.date_created),
            sortable:true,
            center: true
          },
          {
            name: <div></div>,
            button: true,
            cell: (row) => {
              const isApproved = approvalStatus[row.syenappid] === 'approved';
              const isRejected = approvalStatus[row.syenappid] === 'rejected';
        
              return (
                <div className="action-btns" style={{ fontSize: "10px ", fontFamily: "sans-serif", color: 'black'  }}>
                  {!isApproved && !isRejected && !row.isRCMTVerified && (
                    <>
                      <button className="view-address-btn" onClick={() => onApprove(row)} style={{ color: "white", borderRadius: "10px", border: "0px solid white", background: "#008000", width: "90px", height: "28px", borderStyle: "none", cursor: "pointer" }}>
                        Approve
                      </button>
                      <button className="reward-Reject-btn" onClick={() => handleReject(row) } style={{ color: "white", borderRadius: "10px", border: "0px solid white", background: "#f44336", width: "80px", height: "28px", borderStyle: "none", cursor: "pointer", marginLeft: "2px" }}>
                        Reject
                      </button>
                    </>
                  )}
                </div>
              );
            },
            center: true,
          },
    ];

      //function for show & hide the test users
  const func_hideshowTestUser = () => {
    let currentState = document.getElementById("hidetestusers").checked;
    if(currentState){
      let testUsersRemovedData = users.filter(({referredSyenAppId:id1}) => (!testUsersList.some(({syenappId:id2}) => id2 === id1)))
      setUsers(testUsersRemovedData)
    }
    if(!currentState){
      setUsers(totalDetails)
    }
  }

  //checkbox component for show & hide the test users
  const subHeaderComponent1 = (
    <HideTestUsers handleChange={func_hideshowTestUser} checkboxId={'hidetestusers'} />
  )


      //filter
     //filter
     const filteredItems = users.filter(
      (item) =>
        JSON.stringify(item).toLowerCase().indexOf(filterText.toLowerCase()) !==
        -1
    );

    const refresh = <div className="refresh-icon" onClick={handleRefresh}>
    <FaSync className="refresh-icon-svg" />
    <span className="refresh-text">  Refresh</span>
  </div>

      const subHeaderComponent = useMemo(() => {
        const handleClear = () => { 
          if (filterText) {
            setResetPaginationToggle(!resetPaginationToggle);
            setFilterText("");
          }
        };
    
        return (
          <FilterComponent
            onFilter={e => setFilterText(e.target.value)}
            onClear={handleClear}
            filterText={filterText}
          />
        );
      }, [filterText, resetPaginationToggle]);



    const datables = (
        <DataTable
        
            columns={columns}
            refresh={refresh}
            data={filteredItems}
            noDataComponent={"No records found"}
            progressPending={pending}
            customStyles={customStyles}
            pagination
            paginationPerPage = {25}
            paginationRowsPerPageOptions = {[25,50,75,100]}
            subHeader
            subHeaderComponent={[refreshButton, subHeaderComponent1, subHeaderComponent]}
            onChangePage={() => setExpandableRowShow(false)}
            expandableRowExpanded={() => expendableRowShow}
        />
    )
    return (

        <div>
      
            {datables}
         {approve === 'Reject'?rejectionpopup:approvalpopup} 

        </div>

    )

}

export default Refferal;

export const FindReferrals = () => {

    const [referredSyenappId,setReferredSyenappId] = useState('')
    const [referralData,setReferralData] = useState([])
    const [filterText, setFilterText] = useState("");
    const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
    const [pending,setPending] = useState(false)
    const [expendableRowShow,setExpandableRowShow] = useState(false)

    const onGo = (e) => {
        e.preventDefault()
        if(referredSyenappId.trim() !== ''){
            setPending(true)
            axios.get(`https://${apiBaseURL()}/v2/rcmtcontroller/getuserreferralusers?syenappid=${referredSyenappId}`)
            .then((res) => {
                setPending(false)
                // console.log(res.data)
                setReferralData(res.data)
            })
            .catch((err) => {console.log(err);setPending(false);alert('request failed')})
        }
        else{
            alert('Please enter the Referred Syenapp Id')
        }

    }

    const customStyles = {

        noData: {
           style: {
                marginTop: '16%',
                color: '#3b7acc',
                fontSize: '30px',
            },
        },
        subHeader: {
            style: {
        //         display:'contents',
        //   justifyContent:'flex-end',
                minHeight: '52px',
            },
        },
        headRow: {
            style: {
                color:'white',
                fontSize:'15px',
                backgroundColor: ' #3F6EAE',
                minHeight: '52px',
                borderBottomWidth: '1px',
                borderBottomStyle: 'solid',
            },
            denseStyle: {
                  minHeight: '32px',
            },
        },
    }

    const columns = [
        {
            name: <div>Syenapp Id</div>,
            selector: row => row.syenappId,
            sortable:true,
            center: "true"
        },
        {
            name: <div  >FirstName</div>,
            selector: row =>row.first_name,
            center: "true"
        },
        {
            name: <div  >Claimed Date</div>,
            selector: row => row.claimDate,
            center: "true"
        },

        {
            name: <div>Last Session</div>,
            selector: row =>  row.lastSession,
            center: "true"
          
            
        },
        {
          name: <div>Status</div>,
          selector: row => row.status,
          sortable:true,
          center: "true"
      },
     
    ];
    const referSyenappId = ({target})=> setReferredSyenappId(target.value)
    const subheader1 = (
        <form className='find-referral-sub-div'>  
            <div className="referred-id-label">Referred Syenapp Id :</div>
            <input className="referred-id-input"  onChange={referSyenappId} />
            <button onClick={onGo} className="syenapp-id-go-btn" >{pending ? <BtnLoadingSpinner /> : 'Go'}</button>   
        </form>
    )

          //filter
          const filteredItems = referralData.filter(
            (item) =>
              JSON.stringify(item).toLowerCase().indexOf(filterText.toLowerCase()) !==
              -1
          );
         
          const subHeader2 = useMemo(() => {
            const handleClear = () => { 
              if (filterText) {
                setResetPaginationToggle(!resetPaginationToggle);
                setFilterText("");
              }
            };
        
            return (          
              <FilterComponent
              onFilter={e => setFilterText(e.target.value)}
              onClear={handleClear}
              filterText={filterText}
              
            />
            );
          }, [filterText, resetPaginationToggle]);

          const subHeaderComponent = (
            <div className="referral-subheader">
            {subheader1}
            {subHeader2} 
            </div>
          )
       

    return(
        <DataTable
        columns={columns}
        data={filteredItems}
        progressPending={pending}
        noDataComponent={"No records found"}
        customStyles={customStyles}
        defaultSortFieldId={1}
        pagination
        paginationPerPage = {25}
        paginationRowsPerPageOptions = {[25,50,75,100]}
        subHeader
        subHeaderComponent={[subHeaderComponent]}
        onChangePage={() => setExpandableRowShow(false)}
        expandableRowExpanded={() => expendableRowShow}
    />
    )
}
