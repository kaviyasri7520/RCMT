import axios from "axios";
import React, { useState } from "react";
import "./TrackNetwork.css";
import DataTable from "react-data-table-component";
import { cjColumn, fmtcColumn, ImpactColumn, rakutenColumn } from "./columns";
import { apiBaseURL } from "../../core/utils";

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
      backgroundColor: "#333222",
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
      minHeight: "70px", // override the row height
      fontSize: "16px",
      fontWeight: "530",
      textAlign: "center",
      color: "black",
      overflow: "visible",
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
      overflow: "visible",
      wordBreak: "break-all",
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

const TrackFMTC = () => {
  const [pending, setPending] = useState(false);
  const [data, setData] = useState([]);

  const [startDate, setStartDate] = useState("");
  const [endDate, settEndDate] = useState(
    new Date().toISOString().substr(0, 10)
  );
  const [expendableRowShow,setExpandableRowShow] = useState(false)

  const func_inputHandle = (event) => {
    const name = event.target.name;
    name === "start-date" && setStartDate(event.target.value);
    name === "end-date" && settEndDate(event.target.value);
  };

  const onSubmit = () => {
    setPending(true);
    const diffTime = Math.abs(new Date(endDate) - new Date(startDate));
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    // console.log(diffDays)
    axios
      .get(
        `https://${apiBaseURL()}/v2/rcmtcontroller/getfmtcproductdetails?time=-${diffDays}days`
      )
      .then((res) => {
        setPending(false);
        // console.log(res.data)
        setData(res.data);
      })
      .catch((err) => {
        console.log(err);
        setPending(false);
      });
  };

  const Form1 = (
    <div className="date-inputs-container">
      <label>Start Date</label>
      <input
        name="start-date"
        type="date"
        onChange={func_inputHandle}
        value={startDate}
      />
      <label>End Date</label>
      <input
        name="end-date"
        type="date"
        onChange={func_inputHandle}
        value={endDate}
        disabled
      />
      <button type="submit" onClick={onSubmit}>
        Submit
      </button>
    </div>
  );

  return (
    <div>
      {Form1}
      <DataTable
        columns={fmtcColumn}
        customStyles={customStyles}
        data={data}
        progressPending={pending}
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
    </div>
  );
};

export { TrackFMTC };

//R-RAKUTEN, I-IMPACT, C-CJ
const TrackRIC = ({ TabName }) => {
  let currentDate = new Date();
  let today = currentDate.toISOString().substr(0, 10);
  const before30ms = currentDate.setDate(currentDate.getDate() - 1 * 30);
  const before30d = new Date(parseInt(before30ms, 10));
  const before30date = before30d.toISOString().substr(0, 10);
  const [pending, setPending] = useState(false);
  const [startDate, setStartDate] = useState("");
  const [endDate, settEndDate] = useState("");
  const [data, setData] = useState([]);
  const [expendableRowShow,setExpandableRowShow] = useState(false)

  const func_inputHandle = (event) => {
    const name = event.target.name;
    name === "start-date" && setStartDate(event.target.value);
    name === "end-date" && settEndDate(event.target.value);
  };

  const onSubmit = (e) => {
    e.preventDefault();
    if (startDate !== "" && endDate !== "") {
      if (TabName === "RAKUTEN") {
        if (startDate >= before30date && endDate <= today) {
          setPending(true);
          axios
            .get(
              `https://${apiBaseURL()}/v2/rcmtcontroller/getrakutenproductdetails?start=${startDate}&end=${endDate}`
            )
            .then((res) => {
              setPending(false);
              console.log(res.data);
              setData(res.data);
            })
            .catch((err) => {
              console.log(err);
              setPending(false);
            });
        }
        startDate < before30date &&
          alert(`Start date must be ${before30date} or after ${before30date}`);
        endDate > today &&
          alert(`End Date must be ${today} or before ${today}`);
      }
      if (TabName === "IMPACT") {
        setPending(true);
        axios
          .get(
            `https://${apiBaseURL()}/v2/rcmtcontroller/getimpactproductdetails?start=${startDate}&end=${endDate}`
          )
          .then((res) => {
            setPending(false);
            console.log(res.data);
            setData(res.data);
          })
          .catch((err) => {
            console.log(err);
            setPending(false);
          });
      }
      if (TabName === "CJ") {
        setPending(true);
        axios
          .get(
            `https://${apiBaseURL()}/v2/rcmtcontroller/getcjproductdetails?since=${startDate}&post=${endDate}`
          )
          .then((res) => {
            setPending(false);
            console.log(res.data);
            setData(res.data);
          })
          .catch((err) => {
            console.log(err);
            setPending(false);
          });
      }
    }
    startDate === "" && alert("Please select Start date");
    endDate === "" && alert("Please select End Date");
  };

  const Form = (
    <div className="date-inputs-container">
      <label>Start Date</label>
      <input
        name="start-date"
        type="date"
        min={TabName === "RAKUTEN" ? before30date : undefined}
        onChange={func_inputHandle}
        value={startDate}
      />
      <label>End Date</label>
      <input
        name="end-date"
        type="date"
        max={TabName === "RAKUTEN" ? today : undefined}
        onChange={func_inputHandle}
        value={endDate}
      />
      <button type="submit" onClick={onSubmit}>
        Submit
      </button>
    </div>
  );

  const selctColumn = () => {
    if (TabName === "RAKUTEN") {
      return rakutenColumn;
    }
    if (TabName === "IMPACT") {
      return ImpactColumn;
    }
    if (TabName === "CJ") {
      return cjColumn;
    }
  };

  return (
    <div>
      {Form}
      <DataTable
        columns={selctColumn()}
        customStyles={customStyles}
        data={data}
        progressPending={pending}
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
    </div>
  );
};

export { TrackRIC };
