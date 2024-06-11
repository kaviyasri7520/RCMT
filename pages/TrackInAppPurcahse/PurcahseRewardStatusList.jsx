import axios from "axios";
import React, { useEffect, useState, useMemo } from "react";
import FilterComponent from "../../components/filter/FilterComponents";
import DataTable from "react-data-table-component";
import "./PurchaseRewardStatusList.css";
import Modal from "../../components/popUpModals/modal";
import HideTestUsers from "../../components/hideTestUsers/HideTestUsers";
import { apiBaseURL } from "../../core/utils";
const { testUsersURL,apiOcrHeader } = require("../../config/config.json");

const Reward2xStatusList = () => {
  const [details, setDetails] = useState([]);
  const [filterText, setFilterText] = useState("");
  const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
  const [pending, setPending] = useState(false);
  const [rejectReason, setRejectReason] = useState("");
  const [showReject, setShowReject] = useState(false);
  const [rowDetails, setRowDetails] = useState({});
  const [submitTrack, setSubmitTrack] = useState(false);
  const [expendableRowShow,setExpandableRowShow] = useState(false)

  const [testUsersList,setTestUsersList] =  useState([])
  const [totalDetails,setTotalDetails] = useState([])

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

  useEffect(() => {
    setPending(true);
    axios
      .get(`https://${apiBaseURL()}/v2/rcmtcontroller/get2xrewardsstatus`)
      .then((res) => {
        let result = res.data;
  
        // Sort by status first
        result.sort((a, b) => {
          if (a.other.status === "pending" && b.other.status !== "pending") return -1;
          if (a.other.status !== "pending" && b.other.status === "pending") return 1;
          return 0;
        });
  
        // Sort pending status data by createdDate
        result = result.map(item => ({
          ...item,
          other: {
            ...item.other,
            createdDate: item.other.status === 'pending' ? new Date(item.other.createdDate) : item.other.createdDate
          }
        }));
  
        result.sort((a, b) => {
          // If both are pending, sort by createdDate in descending order (most recent first)
          if (a.other.status === "pending" && b.other.status === "pending") {
            return new Date(b.other.createdDate) - new Date(a.other.createdDate);
          }
          return 0; // Leave other statuses unchanged
        });
  
        setDetails(result);
        setPending(false);
        setTotalDetails(result);
      })
      .catch((error) => {
        console.error("Error fetching data:", error);
        setPending(false);
      });
    // Fetch list of test users
    axios
      .get(`${testUsersURL}/testusers-list`)
      .then((res) => {
        const result = res.data;
        setTestUsersList(result);
      })
      .catch((err) => {
        console.log(err);
      });
    document.getElementById("hidetestusers").checked = false;
  }, [submitTrack]);
  
  
  

  const columns = [
    {
      name: <div>Referral Code</div>,
      selector: (row) => row.syenapprefcode,
      sortable: true,
      center: "true",
    },
    {
      name: <div>Flag</div>,
      selector: (row) => row.other.flag,
      center: "true",
    },
    {
      name: <div>Action Date</div>,
      selector: (row) => {
        if (row.other.status === 'pending') {
          return '';
        } else {
          return row.other.status === 'claimed' ? (row.other.claimDate ? new Date(row.other.claimDate).toString().substr(4, 12) || "" : "") : (row.other.rejectDate ? new Date(row.other.rejectDate).toString().substr(4, 12) || "" : "");
        }
      },
      sortable: true,
      center: true,
    },
    {
      name: <div>Reward Status</div>,
      selector: (row) => row.other.status,
      center: "true",
    },
    {
      name: <div>Reward Points</div>,
      selector: (row) => row.other.rewardPoints,
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
          {row.other.status !== "claimed" &&
            row.other.status !== "redeemed" &&
            row.other.status !== "rejected" && (
              <button
                className="view-address-btn"
                onClick={() => func_approve(row)}
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
          {row.other.status !== "claimed" &&
            row.other.status !== "redeemed" &&
            row.other.status !== "rejected" && (
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

  const hidePopup = () => {
    setShowReject(false);
  };

  const func_approve = (row) => {
    const params = {
      mailId: row.syenappid,
      rewardPoints: row.other.rewardPoints,
      status: "claimed",
      flag: row.other.flag,
    };
  
    console.log(params);
  
    axios
      .put(`https://${apiBaseURL()}/v2/rcmtcontroller/updatetaskstatus`, params)
      .then((res) => {
        setSubmitTrack(!submitTrack);
        alert("approved successfully");
        const currentMonthYear = new Date().toISOString().slice(5, 7) + new Date().toISOString().slice(0, 4);
        const existingFlag = `BIRTHPURCHASE_${currentMonthYear}`
        if (row.other.flag === "FIRSTINAPPPURCHASE") {
          axios({
            url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${row.syenappid}&flag=InAppPurchase`,
            method: "get",
            headers: apiOcrHeader,
          })
            .then((res) => {
              if (res.data.data === 1) {
                alert("submitted successfully");
              }
            })
            .catch((err) => {
              console.log(err);
            });
        } else if (row.other.flag === existingFlag) {
          axios({
            url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${row.syenappid}&flag=BIRTHPURCHASE`,
            method: "get",
            headers: apiOcrHeader,
          })
            .then(() => console.log('Birth purchase notification sent'))
            .catch((err) => console.log('Birth purchase notification failed to send'));
        } else {
          axios({
            url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${row.syenappid}&flag=2XREWARDS`,
            method: "get",
            headers: apiOcrHeader,
          })
            .then(() => console.log('2x reward notification sent'))
            .catch((err) => console.log('2x reward notification failed to send'));
        }
      })
      .catch((err) => {
        console.log(err);
      });
  };
  
  const handleReject = (row) => {
    setRowDetails(row);
    setShowReject(true);
  };

  const submitReject = (row) => {
    const params = {
      mailId: rowDetails.syenappid,
      rewardPoints: rowDetails.other.rewardPoints,
      status: "rejected",
      flag: rowDetails.other.flag,
      rejectReason: rejectReason,
    };
    // console.log(params)
   
    if(rejectReason.trim() !== "") {
      axios
        .put(`https://${apiBaseURL()}/v2/rcmtcontroller/updatetaskstatus`, params)
        .then((res) => {
          const result = res.data;
          // console.log(params)
          console.log(result);
          
          alert("rejected successfully");
          axios({
            url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${rowDetails.syenappid}&flag=2XREWARDSREJECT`,
            method: "get",
            headers: apiOcrHeader,
          }).then(() => console.log('2x reward notification sent') ).catch((err) => console.log('2x reward notification failed to sent'))
          setSubmitTrack(!submitTrack);
          setRejectReason("");
          setRowDetails({});
          setShowReject(false);
        })
        .catch((err) => {
          console.log(err);
          setRejectReason("");
        });
    } else {
      alert("Please enter the Reason for Reject");
    }
  };

  //function for show & hide the test users
  const func_hideshowTestUser = () => {
    let currentState = document.getElementById("hidetestusers").checked;
    if(currentState){
      let testUsersRemovedData = details.filter(({syenappid:id1}) => (!testUsersList.some(({syenappId:id2}) => id2 === id1)))
      setDetails(testUsersRemovedData)
    }
    if(!currentState){
      setDetails(totalDetails)
    }
  }

  //checkbox component for show & hide the test users
  const subHeaderComponent1 = (
    <HideTestUsers key='hide-test-users-component' handleChange={func_hideshowTestUser} checkboxId={'hidetestusers'} />
  )

  //filter
  const filteredItems = details.filter(
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
        key = "filter-component"
        onFilter={(e) => setFilterText(e.target.value)}
        onClear={handleClear}
        filterText={filterText}
      />
    );
  }, [filterText, resetPaginationToggle]);

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

  
  const ExpandedComponent = ({ data }) => (
    <table>
      <tbody>
        <tr>
        <th className="expanded-subject-head">Return Days</th>
        <th className="expanded-subject-head">Created Date</th>
          {data.other.status === 'pending' && <th className="expanded-subject-head">Return Closing Date</th>}
          {data.other.status === 'pending' && <th className="expanded-subject-head">Remaining Days</th>}
        </tr>
        <tr>
        <td className="expanded-subject">{data.other.returnDays || ''}</td>
<td className="expanded-subject">{data.other.createdDate ? new Date(data.other.createdDate).toLocaleDateString() : ''}</td>
{data.other.status === 'pending' && <td className="expanded-subject">{data.other.createdDate ? new Date(new Date(data.other.createdDate).setDate(new Date(data.other.createdDate).getDate() + (data.other.returnDays || 0))).toLocaleDateString() : ''}</td>}
{data.other.status === 'pending' && <td className="expanded-subject">{data.other.createdDate ? Math.ceil((new Date(new Date(data.other.createdDate).setDate(new Date(data.other.createdDate).getDate() + (data.other.returnDays || 0))) - new Date()) / (1000 * 60 * 60 * 24)) : ''}</td>}

        </tr>
      </tbody>
    </table>
  )

  return (
    <div>
      <DataTable
        columns={columns}
        customStyles={customStyles}
        data={filteredItems}
        subHeader
        subHeaderComponent={[subHeaderComponent1,subHeaderComponent]}
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
        expandableRowExpanded={() => expendableRowShow}
      />
      {rejectConfirmation_popUp}
    </div>
  );
};

export default Reward2xStatusList;
