import axios from "axios";
import React, { useState, useMemo, useEffect } from "react";
import DataTable from "react-data-table-component";
import FilterComponent from "../../components/filter/FilterComponents";
import Modal from "../../components/popUpModals/modal";
import "./VerifyInstaAccount.css";
import HideTestUsers from "../../components/hideTestUsers/HideTestUsers";
import { apiBaseURL } from "../../core/utils";
const {
  testUsersURL,
  apiStoreHeader,
  apiOcrHeader,
  apiElasticHeader,
} = require("../../config/config.json");

const customStyles = {
  noData: {
    style: {
      marginTop: "16%",
      color: "#3b7acc",
      fontSize: "30px",
    },
  },
  subHeader: {
    style: {
      justifyContent: "flex-end",
      minHeight: "52px",
    },
  },
  headRow: {
    style: {
      color: "white",
      fontSize: "16px",
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
      fontWeight: "530",
      textAlign: "center",
      color: "black",
    },
  },
  headCells: {
    style: {
      width: "3rem ! important",
      color: "#F5F6F6",
      textDecoration: "none",
      fontSize: "16px",
      fontWeight: "520",
      textAlign: "center",
      justifyContent: "center",
    },
  },
  cells: {
    style: {
      width: "3rem ! important",
      paddingLeft: "16px",
      paddingRight: "16px",
      wordBreak: "break-word",
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

const VerifyInstaAccount = () => {
  const [instaData, setInstaData] = useState([]);
  const [pending, setPending] = useState(true);

  const [filterText, setFilterText] = useState("");
  const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
  const [show, setShow] = useState(false);
  const [showReject, setShowReject] = useState(false);
  const [syenappId, setSyenappId] = useState("");
  const [instaAcctName, setInstaAcctName] = useState("");
  const [rewardPoints, setRewardPoints] = useState(null);
  const [rejectReason, setRejectReason] = useState("");
  const [track, setTrack] = useState("1");
  const [expendableRowShow,setExpandableRowShow] = useState(false)

  const [testUsersList,setTestUsersList] =  useState([])
  const [totalDetails,setTotalDetails] = useState([])
  const [currentCountry] = useState(localStorage.getItem("selectedCountry"))

  useEffect(() => {
    const timeout = setTimeout(() => {
      // setRows(users);
      setPending(false);
    }, 2000);
    return () => clearTimeout(timeout);
  }, []);

  useEffect(() => {
    axios({
      url: `https://${apiBaseURL()}/v2/rewards/getinstafollowusers`,
      method: "get",
      Headers: apiStoreHeader,
    })
      .then((res) => {
        const data = res.data.data;
        setInstaData(data);
        setTotalDetails(data);
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
  }, [track]);

  const columns = [
    {
      name: <div>SyenApp ID</div>,
      selector: (row) => row.syenappId,
      sortable: true,
      center: "true",
    },
    {
      name: <div>Reward Points</div>,
      selector: (row) => row.rewardPoints,
      center: "true",
    },
    {
      name: <div>Claim Date</div>,
      selector: (row) =>  new Date(row.claimDate).toString().substr(4, 12),
      sortable: true,
      center: "true",
    },
    {
      name: <div>Insta Account Name</div>,
      selector: (row) => row.instaAcctName,
      center: "true",
    },
    {
      name: <div>Status</div>,
      selector: (row) => row.status,
      center: "true",
    },
    {
      name: <div>Actions</div>,

      selector: (row) => (
        <div
          className="rowdata"
          style={{
            fontSize: "11px ",
            fontFamily: "sans-serif",
            color: "black",
          }}
        >
          <button
         // disabled={currentCountry ==='us' }
            onClick={() => handleApprove(row)}
            style={{
              color: "white",
              borderRadius: "8px",
              border: "0px solid white",
              background: "#008000",
              width: "60px",
              height: "28px",
              borderStyle: "none",
              cursor: "pointer",
              margin: "3px",
            }}
          >
            Approve
          </button>
          <button
            onClick={() => handleReject(row)}
            style={{
              color: "white",
              borderRadius: "8px",
              border: "0px solid white",
              background: "red",
              width: "60px",
              height: "28px",
              borderStyle: "none",
              cursor: "pointer",
              margin: "3px",
            }}
          >
            Reject
          </button>
        </div>
      ),
      center: "true",
    },
  ];

  const handleApprove = (row) => {
    // console.log(row)
    setSyenappId(row.syenappId);
    setInstaAcctName(row.instaAcctName);
    setRewardPoints(row.rewardPoints);
    setShow(true);
  };

  const handleReject = (row) => {
    setSyenappId(row.syenappId);
    setInstaAcctName(row.instaAcctName);
    setShowReject(true);
  };

  const hidePopup = () => {
    setShow(false);
    setShowReject(false);
  };

  const submitApproval = () => {
    console.log(syenappId, instaAcctName, rewardPoints);
    const mongoParams = {
      mailId: syenappId,
      flag: "FOLLOWINSTAGRAM",
      rewardPoints: rewardPoints,
      status: "claimed",
      instaAcctName: instaAcctName,
    };
    const elasticParams = {
      syenappId: syenappId,
      status: "claimed",
      key: "FOLLOWINSTAGRAM",
    };
    axios
      .put(`https://${apiBaseURL()}/v2/rewards/taskstatusapproved`, mongoParams, {
        Headers: apiStoreHeader,
      })
      .then((res) => {
        const data = res.data;
        alert("submitted successfully");
        setTrack(track + "1");
        setShow(false);
        data.status === 1 &&
          axios({
            url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=FOLLOWINSTAGRAM`,
            method: "get",
            headers: apiOcrHeader,
          })
            .then((res) => {
              console.log(res);
            })
            .catch((err) => {
              console.log(err);
            });
      })
      .catch((err) => {
        console.log(err);
        alert("Request failed");
        setShow(false);
      });

    axios
      .put(
        `https://${apiBaseURL()}/elasearch/updateinstagramstatus`,
        elasticParams,
        { headers: apiElasticHeader }
      )
      .then((res) => {
        console.log(res);
      })
      .catch((err) => {
        console.log(err);
        alert("failed to update in elastic search");
      });
  };

  const submitReject = () => {
    // console.log('R',syenappId)

    const mongoParams = {
      mailId: syenappId,
      flag: "FOLLOWINSTAGRAM",
      rewardPoints: rewardPoints,
      status: "rejected",
      instaAcctName: instaAcctName,
      rejectReason: rejectReason,
    };
    const elasticParams = {
      mailId: syenappId,
      flag: "FOLLOWINSTAGRAM",
    };

    if (rejectReason.trim() !== "") {
      axios
        .put(`https://${apiBaseURL()}/v2/rewards/taskstatusremoved`, mongoParams, {
          Headers: apiStoreHeader,
        })
        .then((res) => {
          const data = res.data;
          console.log(data);
          setShowReject(false);
          alert("rejected successfully");
          setTrack(track + "1");
          axios({
            url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=REJECTINSTAFOLLOWUSER`,
            method: "get",
            headers: apiOcrHeader,
          })
            .then((res) => {
              console.log(res);
              setRejectReason("");
            })
            .catch((err) => {
              console.log(err);
            });
        })
        .catch((err) => {
          console.log(err);
          alert("request failed");
        });
      axios
        .put(`https://${apiBaseURL()}/elasearch/deleteusertask`, elasticParams, {
          headers: apiElasticHeader,
        })
        .then((res) => {
          console.log(res);
        })
        .catch((err) => {
          console.log(err);
          alert("failed to update in elatic search");
        });
    } else {
      alert("Please enter the Reason for Reject");
    }
  };

  const confirmation_popUp = (
    <Modal show={show} handleClose={hidePopup}>
      <h2 className="title">Do you want to Continue?</h2>
      <button onClick={() => setShow(false)} className="confirm">
        Cancel
      </button>
      <button onClick={submitApproval} className="cancel">
        Yes,Approve
      </button>
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
          <button onClick={() => setShowReject(false)} className="confirm">
            Cancel
          </button>
          <button onClick={submitReject} className="cancel">
            Yes,Reject
          </button>
        </div>
      </div>
    </Modal>
  );

        //function for show & hide the test users
        const func_hideshowTestUser = () => {
          let currentState = document.getElementById("hidetestusers").checked;
          if(currentState){
            let testUsersRemovedData = instaData.filter(({syenappId:id1}) => (!testUsersList.some(({syenappId:id2}) => id2 === id1)))
            setInstaData(testUsersRemovedData)
          }
          if(!currentState){
            setInstaData(totalDetails)
          }
        }
      
        //checkbox component for show & hide the test users
        const subHeaderComponent1 = (
          <HideTestUsers handleChange={func_hideshowTestUser} checkboxId={'hidetestusers'} />
        )
      

  //filter
  const filteredItems = instaData.filter(
    (item) =>
      JSON.stringify(item).toLowerCase().indexOf(filterText.toLowerCase()) !==
      -1
  );

  //filter component inpput
  const subHeaderComponent = useMemo(() => {
    const handleClear = () => {
      if (filterText) {
        setResetPaginationToggle(!resetPaginationToggle);
        setFilterText("");
      }
    };

    return (
      <FilterComponent
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
        progressPending={pending}
        subHeader
        subHeaderComponent={[subHeaderComponent1,subHeaderComponent]}
        pagination
        paginationRowsPerPageOptions={[25, 50, 75, 100]}
        paginationPerPage={25}
        paginationComponentOptions={{
          rowsPerPageText: "Records per page:",
          rangeSeparatorText: "out of",
        }}
        onChangePage={() => setExpandableRowShow(false)}
        expandableRowExpanded={() => expendableRowShow}
      />
      {confirmation_popUp}
      {rejectConfirmation_popUp}
    </div>
  );
};

export default VerifyInstaAccount;

export const VerifyInstaAccountRewardClaimedUsers = () => {
  const [claimedData, setClaimedData] = useState([]);
  const [pending, setPending] = useState(true);
  const [filterText, setFilterText] = useState("");
  const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
  const [expendableRowShow,setExpandableRowShow] = useState(false)

  const [testUsersList,setTestUsersList] =  useState([])
  const [totalDetails,setTotalDetails] = useState([])

  useEffect(() => {
    axios
      .get(
        `https://${apiBaseURL()}/v2/rewards/getuserrewardstatus?status=claimed&flag=FOLLOWINSTAGRAM`
      )
      .then((res) => {
        const data = res.data;
        setPending(false);
        setClaimedData(data.data);
        setTotalDetails(data.data)
      })
      .catch((err) => {
        console.log(err);
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
  }, []);

  const columns = [
    {
      name: <div>SyenApp ID</div>,
      selector: (row) => row.syenappId,
      sortable: true,
      center: "true",
    },
    {
      name: <div>Insta Account Name</div>,
      selector: (row) => row.instaAcctName,
      center: "true",
    },
    {
      name: <div>Reward Points</div>,
      selector: (row) => row.rewardPoints,
      center: "true",
    },
    {
      name: <div>Claim Date</div>,
      selector: (row) => row.claimDate,
      sortable: true,
      center: "true",
    },
    {
      name: <div>Approved Date</div>,
      selector: (row) => row.aDate,
      center: "true",
    },
    {
      name: <div>Status</div>,
      selector: (row) => row.status,
      center: "true",
    },
    {
      name: <div>from source</div>,
      selector: (row) => row.fromSource,
      center: "true",
    },
  ];

    //function for show & hide the test users
    const func_hideshowTestUser = () => {
      let currentState = document.getElementById("hidetestusers").checked;
      if(currentState){
        let testUsersRemovedData = claimedData.filter(({syenappId:id1}) => (!testUsersList.some(({syenappId:id2}) => id2 === id1)))
        setClaimedData(testUsersRemovedData)
      }
      if(!currentState){
        setClaimedData(totalDetails)
      }
    }
  
    //checkbox component for show & hide the test users
    const subHeaderComponent1 = (
      <HideTestUsers handleChange={func_hideshowTestUser} checkboxId={'hidetestusers'} />
    )

  //filter
  const filteredItems = claimedData.filter(
    (item) =>
      JSON.stringify(item).toLowerCase().indexOf(filterText.toLowerCase()) !==
      -1
  );

  //filter component inpput
  const subHeaderComponent = useMemo(() => {
    const handleClear = () => {
      if (filterText) {
        setResetPaginationToggle(!resetPaginationToggle);
        setFilterText("");
      }
    };

    return (
      <FilterComponent
        onFilter={(e) => setFilterText(e.target.value)}
        onClear={handleClear}
        filterText={filterText}
      />
    );
  }, [filterText, resetPaginationToggle]);

  return (
    <DataTable
      columns={columns}
      customStyles={customStyles}
      data={filteredItems}
      progressPending={pending}
      subHeader
      subHeaderComponent={[subHeaderComponent1,subHeaderComponent]}
      pagination
      paginationRowsPerPageOptions={[25, 50, 75, 100]}
      paginationPerPage={25}
      paginationComponentOptions={{
        rowsPerPageText: "Records per page:",
        rangeSeparatorText: "out of",
      }}
      onChangePage={() => setExpandableRowShow(false)}
      expandableRowExpanded={() => expendableRowShow}
    />
  );
};
