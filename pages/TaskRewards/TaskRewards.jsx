import React, { useState, useEffect, useMemo } from "react";
import axios from "axios";
import FilterComponent from "../../components/filter/FilterComponents";
import DataTable from "react-data-table-component";
import "./TaskRewards.css";
import HideTestUsers from "../../components/hideTestUsers/HideTestUsers";
import { apiBaseURL } from "../../core/utils";

const {
  apiOcrHeader,
  apiElasticHeader,
  testUsersURL,
  apiStoreHeader
} = require("../../config/config.json");

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
      fontSize: "16px",
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

const TaskRewards = () => {
  const [syenappId, SetSyenappId] = useState("");
  const [status, setStatus] = useState("");
  const [tasks, setTasks] = useState([]);
  const [selectedTask, setSelectedTask] = useState("");
  const [rewardPoints, setRewardPoints] = useState("");
  const [track, setTrack] = useState(false);
  const [syenappIdList, setsyenappIdList] = useState([]);
  const [showApprove, setShowApprove] = useState(false);
  const [merchantList, setMerchantList] = useState([])
  const [selectedMerchant, setselectedMerchant] = useState([])
  const [selectedCategoryPoints, setselectedCategoryPoints] = useState([])
 const [totalSyenappList,settotalSyenappList] = useState([])
 const [totalPoints, setTotalPoints]= useState("")
  //table
  const [users, setUsers] = useState([]);
  const [filterText, setFilterText] = React.useState("");
  const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
  const [pending, setPending] = React.useState(true);
  const [expendableRowShow, setExpandableRowShow] = useState(false)

  const [testUsersList, setTestUsersList] = useState([])
  const [totalDetails, setTotalDetails] = useState([])
  const [dynamicRewardPoints, setDynamicRewardPoints] = useState({})
  const [loading, setLoading] = useState(false);

  React.useEffect(() => {
    axios.get(`https://${apiBaseURL()}/v2/rewards/getconfigrewardpoints`)
      .then((res) => {
        const result = res.data.data[0]
        setDynamicRewardPoints(result)
      })
    // .catch((err) => {alert('Cannot fetch the reward points')})
  }, []);

  useEffect(() => {
    axios
      .get(`https://${apiBaseURL()}/v2/rcmtcontroller/syenappidList`)
      .then((res) => {
        const result = res.data;
        let idList = result.map((e) => e.syenapprefcode);
        setsyenappIdList(idList);
        settotalSyenappList(result)
      })
      .catch((error) => {
        //console.log(error);
        //  alert("connection failed");
      });
    axios
      .get(`https://${apiBaseURL()}/v2/rcmtcontroller/taskList`)
      .then((flag) => {
        const taskList = flag.data;
    
        const getTask = taskList.filter(
          (item) => item.key === "COMPLETESURVEYS" || item.key === "FIRSTINAPPPURCHASE" || item.key === "2XREWARDS" || item.key === "APPREVIEW" || item.key === "BIRTHPURCHASE"
        );
        // console.log("specificTask",getTask)
        setTasks(getTask);
      })
      .catch((error) => {
        // console.log(error);
      });
    axios
      .get(`https://${apiBaseURL()}/v2/rcmtcontroller/getusertasklist`)
      //.get(`http://localhost:3020/v2/rcmtcontroller/getusertasklist`)
      .then((res) => {
        const data = res.data;
      

     
        setUsers(data);
        setTotalDetails(data)
        setPending(false);
      })
      .catch((err) => {

      });
    // get list of test users
    axios.get(`${testUsersURL}/testusers-list`)
      .then((res) => {
        let result = res.data
        setTestUsersList(result)
      })
      .catch((err) => { })
    document.getElementById("hidetestusers").checked = false;
  }, [track]);

  const handleSyenappId = (event) => {
    SetSyenappId(event.target.value);
  };

  const handleStatus = ({ target }) => {
    setStatus(target.value);
  };

  const handleTaskChange = ({ target }) => {
    setSelectedTask(target.value);
    const task = tasks.find((task) => task.key === target.value);
    if (task) {
      setRewardPoints(task.rewardPoints);
    } else {
      setRewardPoints("");
    }
    if (target.value === "FIRSTINAPPPURCHASE") {
      setRewardPoints(dynamicRewardPoints.firstInAppPurchase)
      
      setselectedCategoryPoints('');
     
    }
    if (target.value === "2XREWARDS") {
      setRewardPoints("");
    }
    if (target.value === "BIRTHPURCHASE") {
      setRewardPoints("");
    }
    if (target.value === "APPREVIEW") {
      setselectedMerchant(""); // Clear selected merchant
      setselectedCategoryPoints(""); // Clear selected category points
    }
    if (target.value === "COMPLETESURVEYS") {
      setselectedMerchant(""); // Clear selected merchant
      setselectedCategoryPoints(""); // Clear selected category points
    }
 
  };
  const handleMerchantChange = ({ target }) => {
    const selectedOption = target.options[target.selectedIndex];
    console.log("Selected Option:", selectedOption);

    if (selectedTask === "FIRSTINAPPPURCHASE") {
      setselectedMerchant(target.value)
      
      setselectedCategoryPoints('');
     
    }
    if (selectedTask === "2XREWARDS") {
      setselectedMerchant(target.value);
      setselectedCategoryPoints(selectedOption.getAttribute("points"));
    }
    if (selectedTask === "BIRTHPURCHASE"){
      setselectedMerchant(target.value);
    }
    };

  // const handleRewardPointsChange = ({ target }) => {
  //   setRewardPoints(target.value.replace(/[a-zA-Z]/g, "").replace(/[^\w\s]|_/gi, ""));
  // };

 

  const handleRewardPointsChange = ({ target }) => {
   
    if (selectedTask === "BIRTHPURCHASE"){
      setRewardPoints(target.value);
      setselectedCategoryPoints('')
      
    }
    setRewardPoints(target.value.replace(/[^0-9.]/g, ""));
  };


  const handleTotalpoints = ({ target }) => {
   
    if (selectedTask === "BIRTHPURCHASE"){
      setTotalPoints(target.value);
     
      setselectedCategoryPoints('')
      
    }
    
   
  };

  axios({
    url: `https://${apiBaseURL()}/v2/rcmtcontroller/fetchretailerinfo`,
    method: 'get',
   // headers: apiStoreHeader
  }).then((res) => {
    // console.log(res)
    const result = res.data.data;

  if (Array.isArray(result)) {
    // Filter out items where merchantname is null or empty
    const filteredResult = result.filter(item => item.merchantname !== null && item.merchantname !== "");

    // Map the filtered result to options
    let options = filteredResult.map(e => ({
      label: e.merchantname,
      value: e.merchantname,
      points: e.rewardPoints
    }));

    setMerchantList(options);
    }
  })
    .catch((err) => { })

  const handleSubmit = (event) => {
    event.preventDefault();
    setLoading(true);

    if (selectedTask === "2XREWARDS") {
      // Find the corresponding syenapprefcode and syenappid
      const selectedSyenappIdData = totalSyenappList.find(item => item.syenapprefcode === syenappId);
      console.log(selectedSyenappIdData)
      if (selectedSyenappIdData) {
        const { syenapprefcode, syenappid } = selectedSyenappIdData;
        // Now you have syenapprefcode and syenappid, you can proceed with axios request
        const params1 = {
          mailId: syenappId, // syenapprefcode
          status: status,
          merchantName: selectedMerchant,
          source: "RCMTREWARDTASK",
          rewardPoints: parseFloat(rewardPoints) * parseFloat(selectedCategoryPoints),
         // syenappid: syenappid, // Adding syenappid to the params
        };
    
        axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/update2xrewards`, params1)
          .then((res) => {
            const data = res.data.status;
            setLoading(false);
            setTrack(!track);
    
            data === 1 && alert("Submitted Successful");
    if(params1.status === "pending"){
      axios({
        url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappid}&flag=INAPPPURCHASEPENDING&rewardPoints=${rewardPoints}`,
        method: "get",
        headers: apiOcrHeader,
      })
        .then((res) => {
          res.data.status === 1 && SetSyenappId("");
        })
        .catch((err) => {
          //console.log(err);
        });
    }
    else{
      axios({
        url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappid}&flag=2XREWARDS&rewardPoints=${rewardPoints}`,
        method: "get",
        headers: apiOcrHeader,
      })
        .then((res) => {
          res.data.status === 1 && SetSyenappId("");
        })
        .catch((err) => {

        });
    }
            
            SetSyenappId("");
            setSelectedTask("");
            setselectedMerchant("");
            setRewardPoints("");
            setStatus("");
            setselectedCategoryPoints("");
          })
          .catch((err) => {
            alert("Submission failed. Please try again.");
          });
      } else {
        console.log("No data found for selected syenappId:", syenappId);
      }
    }
    else if(selectedTask === "BIRTHPURCHASE"){
  
      const params = {
        mailId: syenappId,
        status: status,
        flag: selectedTask,
        merchantName: selectedMerchant,
        fromSource: "RCMTREWARDTASK",
        rewardPoints: parseFloat(rewardPoints) * parseFloat(selectedCategoryPoints),
        transAmount:parseInt(rewardPoints),
        rewardPoints: totalPoints
       // syenappid: syenappid, // Adding syenappid to the params
      };
      console.log(params)
      axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/updatebirthdayrewards`, params)
      .then((res) => {
        var data= res.data.syenappId
        setLoading(false);
        setTrack(!track);
        alert("Submitted Successful");

        const objParams = {
          "params": {
            syenappId:data,
            status: status,
            key: selectedTask,
            rewardPoints: parseInt(totalPoints),
          },
        };
//elastic search entry
        axios
        .put(`https://${apiBaseURL()}/elasearch/addusertask`, objParams, {
          headers: apiElasticHeader,
        })
        .then(() => {
          

          SetSyenappId("");
          setSelectedTask("");
          setselectedMerchant("");
          setRewardPoints("");
          setStatus("");
          setselectedCategoryPoints("");
          setTotalPoints("")
        })
        .catch((err) => {
          //console.log(err);
          alert("failed to update in elastic search");
        });
        params.flag === 'BIRTHPURCHASE' &&
        axios({
          url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${data}&flag=BIRTHPURCHASEPENDING`,
          method: "get",
          headers: apiOcrHeader,
        })
          .then((res) => {
            res.data.status === 1 && //console.log("notification Sent Successfully")
              // console.log("notification Sent Successfully")
              SetSyenappId("")
          })
          .catch((err) => { });

        
        // Clear form fields after submission
        SetSyenappId("");
        setSelectedTask("");
        setselectedMerchant("");
        setRewardPoints("");
        setStatus("");
        setselectedCategoryPoints("");
        setTotalPoints("");

      })
      .catch((err) => {
        setLoading(false);
        alert("Submission failed. Please try again.");
      });

    }
    
    else {
      const params = {
        syenappid: syenappId,
        status: status,
        flag: selectedTask,
        rewardPoints:  parseInt(rewardPoints),
        merchantName : selectedMerchant,
      };

      
    console.log(params)

      //approve tasks for user api

      let mailIds;
      if (syenappIdList.includes(syenappId)) {
        if (true) {
  
           console.log(params)
    
          axios
            .put(`https://${apiBaseURL()}/v2/rcmtcontroller/addtasklist`, params)
            //.put(`http://localhost:3020/v2/rcmtcontroller/addtasklist`, params)
            .then((res) => {
              const data = res.data.syenappid;
              mailIds = data;
              setTrack(!track);
             alert("Submitted Successful");
              SetSyenappId("")
              const objParams = {
                "params": {
                  syenappId:data,
                  status: status,
                  key: selectedTask,
                  rewardPoints: parseInt(rewardPoints),
                },
              };
           
              //elastic search entry for taskrewards api
              axios
                .put(`https://${apiBaseURL()}/elasearch/addusertask`, objParams, {
                  headers: apiElasticHeader,
                })
                .then(() => {
                  
       
                  SetSyenappId("");
                  setSelectedTask("");
                  setselectedMerchant("");
                  setRewardPoints("");
                  setStatus("");
                  setselectedCategoryPoints("");
                  setTotalPoints("")
                })
                .catch((err) => {
                  //console.log(err);
                  alert("failed to update in elastic search");
                });

              //Send notification to user
              params.flag === 'COMPLETESURVEYS' &&
                axios({
                  url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${mailIds}&flag=COMPLETESURVEYS&rewardPoints=${rewardPoints}`,
                  method: "get",
                  headers: apiOcrHeader,
                })
                  .then((res) => {
                    res.data.status === 1 && //console.log("notification Sent Successfully")
                      // console.log("notification Sent Successfully")
                      SetSyenappId("")
                  })
                  .catch((err) => { });

                 
                    


              params.flag === "FIRSTINAPPPURCHASE" && params.status === "claimed" && axios({
                url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${mailIds}&flag=InAppPurchase&rewardPoints=${rewardPoints}`,
                method: "get",
                headers: apiOcrHeader,
              })
                .then((res) => {
                  res.data.status === 1 &&// console.log("notification Sent Successfully")
                    SetSyenappId("")
                })
                .catch((err) => {
                  //console.log(err);
                });

                params.flag === "FIRSTINAPPPURCHASE" && params.status === "pending" && axios({
                  url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${mailIds}&flag=INAPPPURCHASEPENDING&rewardPoints=${rewardPoints}`,
                  method: "get",
                  headers: apiOcrHeader,
                })
                  .then((res) => {
                    res.data.status === 1 &&// console.log("notification Sent Successfully")
                      SetSyenappId("")
                  })
                  .catch((err) => {
                    //console.log(err);
                  });

                params.flag === 'APPREVIEW' &&
                axios({
                  url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${mailIds}&flag=APPREVIEW&rewardPoints=${rewardPoints}`,
                  method: "get",
                  headers: apiOcrHeader,
                })
                  .then((res) => {
                    console.log("fetched ID",mailIds)
                
                    res.data.status === 1 && //console.log("notification Sent Successfully")
                      // console.log("notification Sent Successfully")
                      SetSyenappId("")
                  })
                  .catch((err) => { });
            })
            .catch((err) => {
              console.log("issue",err)
       
              alert("request failed",err);
            });
        }

      } else {
        alert("Please enter the valid syenappId");
      }
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
      name: <div>Tasks</div>,
      selector: (row) => row.flag,
      center: "true",
    },
    {
      name: <div>Reward Points</div>,
      selector: (row) => row.rewardPoints,
      center: "true",
    },
    {
      name: <div>Status</div>,
      selector: (row) => row.status,
      center: "true",
    },
    {
      name: <div>Claim Date</div>,
      selector: (row) => row.claimDate ? new Date(row.claimDate).toString().substr(4, 12) : '',
      center: "true",
      sortable: true,
    },
  ];
  //function for show & hide the test users
  const func_hideshowTestUser = () => {
    let currentState = document.getElementById("hidetestusers").checked;
    if (currentState) {
      let testUsersRemovedData = users.filter(({ syenappid: id1 }) => (!testUsersList.some(({ syenappId: id2 }) => id2 === id1)))
      setUsers(testUsersRemovedData)
    }
    if (!currentState) {
      setUsers(totalDetails)
    }
  }

  //checkbox component for show & hide the test users
  const subHeaderComponent1 = (
    <HideTestUsers key="test-user-component" handleChange={func_hideshowTestUser} checkboxId={'hidetestusers'} />
  )

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

  const datatables = (
    <DataTable
      style={{ marginTop: "-7%" }}
      columns={columns}
      data={filteredItems}
      noDataComponent={"No records found"}
      customStyles={customStyles}
      progressPending={pending}
      pagination
      paginationRowsPerPageOptions={[25, 50, 75, 100]}
      paginationPerPage={25}
      subHeader
      subHeaderComponent={[subHeaderComponent1, subHeaderComponent]}
      onChangePage={() => setExpandableRowShow(false)}
      expandableRowExpanded={() => expendableRowShow}
    />
  );
  const handleApprove = () => {
    // Handle the approval logic here
    //console.log("Approved!");
    // Close the modal
    setShowApprove(false);
  };
  return (
    <div className="page-container">
      <div>
        {" "}
        <form onSubmit={handleSubmit} className="form-horizontal">
          <div className="form-group">
            <input
              placeholder="Enter Refcode"
              required
              style={{ height: "30px", width: "150px" }}
              type="tel"
              id="syenappId"
              value={syenappId}
              onChange={handleSyenappId}
              autoComplete="off"
            />
          </div>

          <div className="form-group">
            <select
              required
              style={{ width: "150px", height: "35px" }}
              id="task"
              value={selectedTask}
              onChange={handleTaskChange}
              
             
            >
              <option value="" >Select a Task </option>
              {tasks.map((task) => (
                <option key={task.key} value={task.key}>
                  {task.key}
                </option>
              ))}
            </select>
          </div>
          <div className="form-group">
            <select
            
            disabled={ selectedTask === "COMPLETESURVEYS" || selectedTask === "APPREVIEW"  }
              required
              style={{ width: "160px", height: "35px" }}
              id="task"
              value={selectedMerchant}
              onChange={handleMerchantChange}
            >
              <option value=""> Select a Merchant </option>
              {merchantList.map((task) => {
                return (
                  <option key={task.value} value={task.value} points={task.points}   disabled={
                    // selectedTask === "FIRSTINAPPPURCHASE" ||
                     
                    selectedTask === "COMPLETESURVEYS" ||
                    selectedTask === "APPREVIEW" 
                  }>
                    {task.value}
                  </option>
                );
              })}

            </select>
          </div>

          <input
            disabled={selectedTask === "FIRSTINAPPPURCHASE" || selectedTask === "COMPLETESURVEYS" || selectedTask === "APPREVIEW"  || selectedTask === "BIRTHPURCHASE"}
            placeholder="Category Points"
            required
            style={{ height: "30px", width: "160px" }}
            id="CategoryPonits"
            value={selectedCategoryPoints}
            //onChange={handleRewardPointsChange}
            autoComplete="off"
          // disabled={selectedTask === "2XREWARDS"}
          />



          <input
            placeholder="Purchase Amount"
            required
            style={{ height: "30px", width: "160px" }}
            id="rewardPoints"
            value={rewardPoints}
            onChange={handleRewardPointsChange}
            autoComplete="off"
        
          />

<input
          disabled={selectedTask === "FIRSTINAPPPURCHASE" || selectedTask === "COMPLETESURVEYS" || selectedTask === "APPREVIEW"}
            placeholder="Total Points"
            required
            style={{ height: "30px" , width:"110px"}}
            id="CategoryPonits"
            value={
              selectedTask === "2XREWARDS"
                ? rewardPoints * selectedCategoryPoints
                : totalPoints
            }
            onChange={handleTotalpoints}
            autoComplete="off"
           // disabled={selectedTask === "2XREWARDS"}
          />

          <select
            style={{ width: "150px", height: "35px" }}
            className="form-control"
            value={status}
            onChange={handleStatus}
            // disabled={selectedTask === "2XREWARDS"}
            required
          >
            <option style={{ display: "none" }} value="">
              Select Status
            </option>
            <option key={1} value="pending">
              Pending
            </option>
            <option key={2} value="claimed">
              Claimed
            </option>
          </select>

          <div>
            {/* <button
              style={{ height: "35px" }}
              type="submit"
              className="btn btn-primary"
            >
              Submit
            </button> */}
            <button className='bs-submit' type="submit" >Submit</button>
          </div>
        </form>
      </div>

      {showApprove && (
        <div className="modal">
          <div className="modal-content">
            <span className="close" onClick={() => setShowApprove(false)}>
              &times;
            </span>
            <h2>Handle Approve</h2>
            <p>Do you want to approve?</p>
            <button onClick={handleApprove}>Approve</button>
          </div>
        </div>
      )}
      {datatables}
    </div>
  );
};

export default TaskRewards;
