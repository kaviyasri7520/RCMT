import { useEffect, useState } from "react";
import React, { useMemo } from "react";
import "./RewardsTrack.css";
import axios from "axios";
import DataTable from "react-data-table-component";
import Modal from "../../components/popUpModals/modal";
import FilterComponent from "../../components/filter/FilterComponents";
import HideTestUsers from "../../components/hideTestUsers/HideTestUsers";
import { apiBaseURL } from "../../core/utils";
import BtnLoadingSpinner from "../../components/loadingSpinner/BtnLoadingSpinner";

const {  apiOcrHeader, testUsersURL } = require('../../config/config.json')

const RewardsTrack = () => {
  const [filterText, setFilterText] = React.useState("");
  const [resetPaginationToggle, setResetPaginationToggle] =
    React.useState(false);
  const [users, setUsers] = useState([]);
  const [syenappId, setSyenappId] = useState("");
  const [key, setKey] = useState("");
  const [show, setShow] = useState(false);
  const [approve, setApprove] = useState("");
  const [track, setTrack] = useState("");
  const [status, setStatus] = useState("");
  const [reason, setReason] = useState("");
  const [rejectSelect, setRejectSelect] = useState(false);
  const [pending, setPending] = React.useState(true);
  const [expendableRowShow,setExpandableRowShow] = useState(false)

  const [testUsersList,setTestUsersList] =  useState([])
  const [totalUsers,setTotalUsers] = useState([])
  const [currentCountry] = useState(localStorage.getItem("selectedCountry"))

  function country(){
    if(currentCountry === "us"){
      return "us"
    }
    else{
      return "in"
    }
  }

  function symbol(){
    if(currentCountry === "us"){
        return "$"
    }
    else{
        return "₹"
    }
  }

  useEffect(() => {
    axios
      .get(
        `https://${apiBaseURL()}/sygappbckoffice/getredeemrewarddetails?status=pending&country=${country()}`
      )
      .then((res) => {
        setPending(false);
        const data = res.data;
        const data1 = data.data;
       
         data1.sort((a, b) => new Date(b.applyDate) - new Date(a.applyDate));
        
        setUsers(data1);
        setTotalUsers(data1)
      })
      .catch((error) => {
        console.log(error);
        setPending(false);
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
  }, [track]);


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
        fontSize: "18px",
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
        fontSize: "16px",
        textAlign: "center",
        color: "black",
      },
    },
    headCells: {
      style: {
        color: "#F5F6F6",
        textDecoration: "none",
        fontSize: "17px",
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
  };

  const handleReject = (syenappId, key, reason) => {
    const state = "rejected";
    setRejectSelect(true);
    setSyenappId(syenappId);
    setKey(key);
    setStatus(state);
    showrejPopup();
    setApprove("Reject");
  };

  const onchanges = (event) => {
    setReason(event.target.value);
  };

  const handleApprove = (syenappId, key) => {
    const state = "approved";
    setSyenappId(syenappId);
    setKey(key);
    setStatus(state);

    showsPopup();
    setApprove("Approve");
    // axios({
    //   url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=REWARDSTRACKAPPROVE`,
    //   method: "get",
    //   headers: apiOcrHeader,
    // }).then(() => console.log('Approved Rewards notification sent') ).catch((err) => console.log('Approved Rewards notification failed to sent'))

  };
  // debugger;

  const handleapi = () => {
    const param = {
      syenappId: syenappId,
      key: key,
      status: status,
      reason: reason,
    };
    console.log(param)
    if (!rejectSelect || reason.trim() !== "") {
      axios
        .put(
          `https://${apiBaseURL()}/sygappbckoffice/updateRedeemHistoryDetails`,
          param
        )
        .then((res) => {
          setReason("");
          setRejectSelect(false);
          // const update=res.data;
          // console.log("handleapi Triggered",update)
          if (res) {
            alert("Submitted Successfully");
            if(status === "approved"){
              axios({
                url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=REWARDSTRACKAPPROVE`,
                method: "get",
                headers: apiOcrHeader,
              }).then(() => console.log('Approved Rewards notification sent') ).catch((err) => console.log('Approved Rewards notification failed to sent'))
              hidePopup();
              setTrack(track + 1);
            }
            else{
              axios({
                url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=REWARDSTRACKREJECT`,
                method: "get",
                headers: apiOcrHeader,
              }).then(() => console.log('Approved Rewards notification sent') ).catch((err) => console.log('Approved Rewards notification failed to sent'))
          
              hidePopup();
              setTrack(track + 1);
            }
          
          }
        })
        .catch((error) => {
          console.log(error);
          alert("request failed");
        });
    } else {
      alert("Please enter the Reason");
    }
  };

  const showsPopup = () => {
    setShow(true);
  };

  const hidePopup = () => {
    setShow(false);
  };
  const showrejPopup = () => {
    setShow(true);
  };

  const hiderejPopup = () => {
    setRejectSelect(false);
    setShow(false);
  };

  const updatePopup = (
    <Modal show={show} handleClose={hidePopup}>
      <div className="empty"></div>
      <h2 className="title">Do you want to Approve?</h2>
      <button onClick={hidePopup} className="confirm">
        Cancel
      </button>
      <button onClick={handleapi} className="cancel">
        Approve
      </button>
    </Modal>
  );
  const updaterejPopup = (
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
      <button onClick={hiderejPopup} className="confirm">
        Cancel
      </button>
      <button onClick={handleapi} className="cancel">
        Reject
      </button>
    </Modal>
  );

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
      name: <div>Referral Code</div>,
      selector: (row) => row.syenapprefcode,
      sortable: true,
      center: "true",
    },
    {
      name: <div>Total Points</div>,
      selector: (row) => row.totalRewardPts,
      center: "true",
    },
    {
      name: <div>Redeemed Points</div>,
      selector: (row) => row.approvedRewardPts,
      center: "true",
    },

    {
      name: <div>Status</div>,
      selector: (row) => row.status,
      center: "true",
    },
    {
      name: <div>Applied Date</div>,
      selector: (row) =>formatDate(row.applyDate) ,
      center: "true",
      sortable:true
   },
   
    {
      name: <div></div>,
      button: true,
      cell: (row) =>
        row.status === "pending" && (
          <div
          className="action-btns"
          style={{
            fontSize: "11px ",
            fontFamily: "sans-serif",
            color: "black",
          }}
          >
            <button
              className="view-address-btn"
              onClick={() => handleApprove(row.syenappId, row.key, status)}  
            >
              Approve
            </button>
          </div>
        ),
      center: "true",
    },
    {
      name: <div></div>,
      button: true,
      cell: (row) =>
        row.status === "pending" && (
          <div
          className="action-btns"
          style={{
            fontSize: "11px ",
            fontFamily: "sans-serif",
            color: "black",
          }}
          >
            <button
              onClick={() => handleReject(row.syenappId, row.key, status)}
              className="reward-Reject-btn"
            >
              Reject
            </button>
          </div>
        ),
      center: "true",
    }
  ];

  //filter
  const filteredItems = users.filter(
    (item) =>
      JSON.stringify(item).toLowerCase().indexOf(filterText.toLowerCase()) !==
      -1
  );
  const subHeaderComponent = useMemo(() => {
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

  const ExpandedComponent = ({ data }) => (
    <table>
      <tbody>
        <tr>
          <th className="thead">Redeem Details</th>
          <th className="thead">Points Details</th>
          {data.payeeName && data.payeeAccount && (
            <th className="thead">Payee Details</th>
          )}
          {data.status !== "pending" && (
            <th className="thead">Approved Details</th>
          )}
        </tr>
        <tr>
          <td className="tdata">Amount {symbol()}: {data.amount}</td>
          <td className="tdatas">Refferal points : {data.refferalRedeemPts}</td>
          {data.payeeName && data.payeeAccount && (
            <td className="tdatas">Payee Name : {data.payeeName}</td>
          )}
          {data.status !== "pending" && (
            <td className="tdatas">
              {data.status === "rejected"
                ? "Rejected Date : "
                : "Approved Date : "}
              {new Date(data.approveDate).toString().substr(4, 12)}
            </td>
          )}
        </tr>
        <tr>
          <td className="tdata">
            Apply Date : {new Date(data.applyDate).toString().substr(4, 12)}
          </td>
          <td className="tdatas">
            Subscription Points : {data.subscribedRedeemPts}
          </td>
          {data.payeeName && data.payeeAccount && (
            <td className="tdatas">Payee Account : {data.payeeAccount}</td>
          )}
          {data.status === "rejected" && (
            <td className="tdatas">Reason : {data.reason}</td>
          )}
        </tr>
        <tr>
          <td className="tdata">Source : {data.source}</td>
          <td className="tdatas">Balance redeem points : {data.pendingRedeemPts}</td>
        </tr>
        <tr>
          <td className="tdata">Payment Type : {data.paymentType}</td>
          <td className="tdata"> -</td>
        </tr>
      </tbody>
    </table>
  );

    //function for show & hide the test users
    const func_hideshowTestUser = () => {
      let currentState = document.getElementById("hidetestusers").checked;
      console.log(currentState)
      if(currentState){
        let testUsersRemovedData = users.filter(({syenappId:id1}) => (!testUsersList.some(({syenappId:id2}) => id2 === id1)))
        setUsers(testUsersRemovedData)
      }
      if(!currentState){
        setUsers(totalUsers)
      }
    }
  
    //checkbox component for show & hide the test users
    const subHeaderComponent1 = (
      <HideTestUsers key='test-users-component' handleChange={func_hideshowTestUser} checkboxId={'hidetestusers'} />
    )

  const datables = (
    <DataTable
      columns={columns}
      data={filteredItems}
      expandableRows
      ExpandedComponent
      paginationRowsPerPageOptions={[25, 50, 75, 100]}
      expandableRowsComponent={ExpandedComponent}
      customStyles={customStyles}
      noDataComponent={"No records found"}
      paginationPerPage={25}
      subHeader
      subHeaderComponent={[subHeaderComponent1,subHeaderComponent]}
      pagination
      progressPending={pending}
      paginationComponentOptions={{
        rowsPerPageText: "Records per page:",
        rangeSeparatorText: "out of",
      }}
      onChangePage={() => setExpandableRowShow(false)}
      expandableRowExpanded={() => expendableRowShow}
    />
  );

  return (
    <div className="page-container">
      {datables}
      {approve === "Reject" ? updaterejPopup : updatePopup}
    </div>
  );
};

export default RewardsTrack;




export const PaymentStatusTable = () => {
  const [referenceId, setReferenceId] = useState('');
  const [paymentData, setPaymentData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleFetchData = async () => {
    if (!referenceId.trim()) {
      setError('Please enter a reference ID');
      return;
    }

    setLoading(true);
    setError('');

    try {
      const response = await axios.put(`http://34.93.254.75:5002/paymentfetchstatus`, {
        reference_id: referenceId
      });
      setPaymentData(response.data.data);
      console.log(response.data)
    } catch (error) {
      setError('Error fetching payment status. Please try again later.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="payment-status-container">
      <form className="payment-form" onSubmit={(e) => { e.preventDefault(); handleFetchData(); }}>
        <label>
          Reference ID:
          
          <input type="text" value={referenceId} onChange={(e) => setReferenceId(e.target.value)} />
        </label>
        <button type="submit" className="fetch-button" disabled={loading}>{loading ? 'Loading...' : 'Submit'}</button>
      </form>
      
      {error && <div className="error-message">{error}</div>}
      
      {paymentData && (
  <table className="payment-table">
    <thead>
      <tr>
        {Object.keys(paymentData).map(key => (
          <th key={key}>{key}</th>
        ))}
      </tr>
    </thead>
    <tbody>
      <tr>
        {Object.values(paymentData).map(value => (
          <td key={value}>{value}</td>
        ))}
      </tr>
    </tbody>
  </table>
)}
    </div>
  );
};









//   const [referredSyenappId, setReferredSyenappId] = useState('');
//   const [referralData, setReferralData] = useState(null);
//   const [pending, setPending] = useState(false);

//   const onGo = (e) => {
//     e.preventDefault();
//     if (referredSyenappId.trim() !== '') {
//       setPending(true);
//       var params = {
//         "reference_id": referredSyenappId
//       };
//       axios.put(`http://192.168.1.201:3020/sygappbckoffice/paymentfetchstatus`, params)
//         .then((res) => {
//           setPending(false);
//           // Assuming the data you need is in res.data.data
//           setReferralData(res.data);
//         })
//         .catch((err) => {
//           console.log(err);
//           setPending(false);
//           alert('Request failed');
//         });
//     } else {
//       alert('Please enter the Reference ID');
//     }
//   };

//   const columns = [
//     {
//       name: "Transcation Id",
//       selector: "transcation_id",
//       sortable: true,
//       center: true
//     },
//     {
//       name: "Reference Id",
//       selector: "reference_id",
//       center: true
//     },
//     {
//       name: "Claimed Date",
//       selector: "createdAt",
//       center: true
//     },
//     {
//       name: "Last Session",
//       selector: "lastSession",
//       center: true
//     }
//   ];

//   const referSyenappId = ({ target }) => setReferredSyenappId(target.value);

//   const subheader1 = (
//     <form className='find-referral-sub-div'>
//       <div className="referred-id-label">Reference Id :</div>
//       <input className="referred-id-input" onChange={referSyenappId} />
//       <button onClick={onGo} className="syenapp-id-go-btn" >{pending ? 'Loading...' : 'Go'}</button>
//     </form>
//   );

//   return (
//     <>
//       <form className='find-referral-sub-div'>
//         <div className="referred-id-label">Reference Id :</div>
//         <input className="referred-id-input" onChange={referSyenappId} />
//         <button onClick={onGo} className="syenapp-id-go-btn" >{pending ? 'Loading...' : 'Go'}</button>
//       </form>

//       {referralData &&
//         <DataTable
//           columns={columns}
//           data={[referralData]} // Wrap the referralData in an array
//           progressPending={pending}
//           noDataComponent={"No records found"}
//           defaultSortFieldId={1}
//           subHeader
//           subHeaderComponent={<div className="referral-subheader">{subheader1}</div>}
//         />
//       }
//     </>
//   );
// };

