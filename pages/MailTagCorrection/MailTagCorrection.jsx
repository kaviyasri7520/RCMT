import React, { useState, useMemo, useRef } from "react";
import DataTable from "react-data-table-component";
import FilterComponent from "../../components/filter/FilterComponents";
import "./MailTagCorrection.css";
import { RxMagnifyingGlass } from "react-icons/rx";
import { apiBaseURL } from "../../core/utils";
import axios from "axios";
const { apiElasticHeader } = require("../../config/config.json");

const MailTagCorrection = () => {
  const [filterText, setFilterText] = useState("");
  const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
  const [choosenMailTag, setChoosenMailTag] = useState("");
  const [days, setDays] = useState("");
  const [datas, setDatas] = useState([]);
  const [trackSubmit, setTrackSubmit] = useState("1");
  const [pending, setPending] = React.useState(true);
  const [expendableRowShow,setExpandableRowShow] = useState(false)
  const [refreshCounter, setRefreshCounter] = useState(0); // Counter for triggering refresh

  React.useEffect(() => {
    try {
      setPending(true);
      axios({
        url: `https://${apiBaseURL()}/elasearch/getmailconversationsbytag?mailTag=${choosenMailTag}&dateFilter=${days}`,
        method: "get",
        headers: apiElasticHeader,
      })
        .then((res) => {
          const output = res.data.result;
          // console.log(output)
          setDatas(output);
          setPending(false);
        })
        .catch((err) => {
          console.log(err);
          alert("connection failed");
        });
    } catch (err) {
      console.log(err);
    }
  }, [trackSubmit,refreshCounter]);

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
        justifyContent: "flex-start",
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
        minHeight: "52px", // override the row height
        fontSize: "13px",
        fontWeight: "530",
        textAlign: "center",
        color: "black",
      },
    },
    headCells: {
      style: {
        // width:'2rem ! important',
        color: "#F5F6F6",
        textDecoration: "none",
        fontSize: "14px",
        fontWeight: "520",
        textAlign: "center",
        justifyContent: "center",
        padding: "0",
        // width:'fit-content'
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
        fontSize: "12px",
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

  const columns = [
    {
      name: <div>SyenApp ID</div>,
      selector: (row) => row.syenappid,
    },
    {
      name: <div>Merchant Name</div>,
      selector: (row) => (
        <div title={row.merchant_name}>{row.merchant_name}</div>
      ),
      center: true,
    },
    {
      name: <div>Subject</div>,
      selector: (row) => <div title={row.c_p_name}>{row.c_p_name}</div>,
      center: "true",
    },
    {
      name: <div>Mail Tag</div>,
      selector: (row) => row.mailtag,
      center: "true",
    },
    {
      name: <div>New Mail Tag</div>,
      cell: (row, i) => (
        <select
          className="mi-status-modify"
          name="status"
          id="status"
          value={row.newMailTag !== undefined ? row.newMailTag : ""}
          onChange={(e) => func_selTag(row, e, i)}
          placeholder="please select"
        >
          <option style={{ display: "none" }} value=""></option>
          <option value="order delivery">order delivery</option>
          <option value="order update">order update</option>
          <option value="order confirmation">order confirmation</option>
          <option value="order refund">order refund</option>
          <option value="order cancellation">order cancellation</option>
          <option value="ereceipts">ereceipts</option>
          <option value="welcome email">welcome email</option>
          <option value="activation emails">activation emails</option>
          <option value="coupons">coupons</option>
          <option value="rewards">rewards</option>
          <option value="promotions">promotions</option>
          <option value="newsletter">newsletter</option>
          <option value="support email">support email</option>
          <option value="others">others</option>
        </select>
      ),
      center: "true",
      // width:"12rem",
      // style:{padding:'0',marginLeft:'1rem'}
    },
    {
      name: <div>Action</div>,
      selector: (row) =>
        row.newMailTag !== "" && row.newMailTag !== undefined ? (
          <button
            className="mail-tag-correction-btn"
            onClick={() => func_changeTagSubmit(row)}
          >
            Submit
          </button>
        ) : (
          <button className="mail-tag-correction-btn-disabled" disabled>
            Submit
          </button>
        ),
      center: "true",
      // width:"12rem",
      style: { padding: "0" },
    },
  ];

  const funct_chooseTag = (event) => {
    setChoosenMailTag(event.target.value);
  };

  // const funct_chooseOption = (event) => {
  //   setOption(event.target.value)
  // }
  const func_selTag = (row, event, i) => {
    let newData = [...datas];
    let NewMailTag = event.target.value;
    const updatedMailTagArray = newData.map((p) =>
      p.messageid === row.messageid ? { ...p, newMailTag: NewMailTag } : p
    );
    setDatas(updatedMailTagArray);
  };

  const func_days = (event) => {
    setDays(
      event.target.value.replace(/[a-zA-Z]/g, "").replace(/[^\w\s]|_/gi, "")
    );
  };

  const func_search = () => {
    if (days >= 1 && choosenMailTag !== "") {
      setPending(true);
      axios({
        url: `https://${apiBaseURL()}/elasearch/getmailconversationsbytag?mailTag=${choosenMailTag}&dateFilter=${days}`,
        method: "get",
        headers: apiElasticHeader,
      })
        .then((res) => {
          const output = res.data.result;
          setDatas(output);
          setPending(false);
          // console.log(output)
        })
        .catch((err) => {
          console.log(err);
          alert("connection failed");
        });
    } else {
      if (choosenMailTag === "") {
        alert("Please select the Mail Tag");
      }
      if (days < 1) {
        alert("Please enter the number of days");
      }
    }
  };
  const searchButtonRef = useRef(null);
  const func_changeTagSubmit = (row) => {
    // console.log(row)
    const params = {
      syenappid: row.syenappid,
      retailid: row.retailid,
      merchant_email: row.merchant_email,
      received_date: row.received_date,
      processed_date: row.processed_date,
      c_p_name: row.c_p_name,
      fromMailType: row.mailtype,
      fromMailTag: row.mailtag,
      toMailTag: row.newMailTag,
      encoding_type: row.encoding_type,
      processed_path: row.processed_path,
      messageid: row.messageid,
    };
    const revertParams = {
      syenappid: row.syenappid,
      retailid: row.retailid,
      merchant_email: row.merchant_email,
      received_date: row.received_date,
      processed_date: row.processed_date,
      c_p_name: row.c_p_name,
      fromMailType: row.mailtype,
      fromMailTag: row.newMailTag,
      toMailTag: row.mailTag,
      encoding_type: row.encoding_type,
      processed_path: row.processed_path,
      messageid: row.messageid,
    };
    //update in mongob and elastic search
    let tagChangedRow = {
      messageId: row.messageid,
      mailTag: row.newMailTag,
      oldMailTag: row.mailtag,
      employeeName: localStorage.getItem("employeeName"),
    };
    console.log(tagChangedRow, localStorage.getItem("employeeName"));
    axios
      .put(
        `https://${apiBaseURL()}/v2/rcmtcontroller/updatemailconversationmpsmaster`,
        params
      )
      .then((res) => {
        const status = res.data.status;
        setDatas(prevDatas => prevDatas.filter(item => item.messageid !== row.messageid));

        status === 1 && alert("updated successfully");
        setTimeout(() => {
          searchButtonRef.current.click();
        }, 1000);

        status === -1 &&
          alert("failed to update the record in mongoDB with status -1");
        status === 0 &&
          alert("failed to update the record in mongoDB  with status 0");
        if (res.data.status === 1) {
          axios
            .put(
              `https://${apiBaseURL()}/elasearch/updatemailconversationsbytag`,
              tagChangedRow,
              { headers: apiElasticHeader }
            )
            .then((res) => {
              let status1 = res.data.status;
              status1 === 1 && setTrackSubmit(trackSubmit + 1);
              res.data.status === 0 &&
                alert(
                  "failed to update the record in elastic search with status 0"
                );
              res.data.status === 0 &&
                axios
                  .put(
                    `https://${apiBaseURL()}/v2/rcmtcontroller/updatemailconversationmpsmaster`,
                    revertParams
                  )
                  .then((res) => console.log(res))
                  .catch((err) => {
                    console.log("mongodb", err);
                    alert("failed to revert in mongodb");
                  });
            })
            .catch((err) => {
              console.log("elastic", err);
              alert(
                "failed to update the record in elastic search - exception occured"
              );
              axios
                .put(
                  `https://${apiBaseURL()}/v2/rcmtcontroller/updatemailconversationmpsmaster`,
                  revertParams
                )
                .then((res) => console.log(res))
                .catch((err) => {
                  console.log("mongodb", err);
                  alert("failed to revert in mongodb - exception occured");
                });
            });

        }
      })
      .catch((err) => {
        console.log("mongodb", err);
        alert("failed to update the record in mongoDB - exception occured");
      });
  };

  //filter
  const filteredItems = datas.filter(
    (item) =>
      JSON.stringify(item).toLowerCase().indexOf(filterText.toLowerCase()) !==
      -1
  );

  const subHeaderComponent1 = (
    <div className="subheader-div">
      <label for="tags">Select a Mail Tag</label>
      <select
        className="mail-tag-select"
        name="tags"
        id="tags"
        onChange={funct_chooseTag}
        placeholder="please select"
      >
        <option style={{ display: "none" }} value=""></option>
        <option value="order delivery">order delivery</option>
        <option value="order update">order update</option>
        <option value="order confirmation">order confirmation</option>
        <option value="order refund">order refund</option>
        <option value="order cancellation">order cancellation</option>
        <option value="ereceipts">ereceipts</option>
        <option value="welcome email">welcome email</option>
        <option value="activation emails">activation emails</option>
        <option value="coupons">coupons</option>
        <option value="rewards">rewards</option>
        <option value="promotions">promotions</option>
        <option value="newsletter">newsletter</option>
        <option value="support email">support email</option>
        <option value="others">others</option>
      </select>
      <label for="tag-date">Enter the number of days</label>
      <input
        className="mail-date-input"
        name="tag-date"
        id="tag-date"
        value={days}
        onChange={(e) => func_days(e)}
        autoComplete="off"
      />

      <button className="search-button" type="submit" onClick={func_search}>
        Search <RxMagnifyingGlass className="search-icon" />
      </button>
    </div>
  );

  const subHeaderComponent2 = useMemo(() => {
    const handleClear = () => {
      if (filterText) {
        setResetPaginationToggle(!resetPaginationToggle);
        setFilterText("");
      }
    };

    return (
      <div className="filter-input-box">
        <FilterComponent
          onFilter={(e) => setFilterText(e.target.value)}
          onClear={handleClear}
          filterText={filterText}
        />{" "}
      </div>
    );
  }, [filterText, resetPaginationToggle]);

  const ExpandedComponent = ({ data }) => (
    <table>
      <tbody>
        <tr>
          <th style={{ width: "50%" }} className="expanded-subject-head">
            Message Id
          </th>
          <th style={{ width: "50%" }} className="expanded-subject-head">
            Subject
          </th>
        </tr>
        <tr>
          <td className="expanded-subject">{data.messageid}</td>
          <td className="expanded-subject">{data.c_p_name}</td>
        </tr>
      </tbody>
    </table>
  );

  const datables = (
    <DataTable
      columns={columns}
      data={filteredItems}
      expandableRows
      expandableRowsComponent={ExpandedComponent}
      paginationPerPage={25}
      paginationRowsPerPageOptions={[25, 50, 75, 100]}
      customStyles={customStyles}
      noDataComponent={"No records found"}
      subHeader
      progressPending={pending}
      subHeaderComponent={[subHeaderComponent1, subHeaderComponent2]}
      pagination
      paginationComponentOptions={{
        rowsPerPageText: "Records per page:",
        rangeSeparatorText: "out of",
      }}
      onChangePage={() => setExpandableRowShow(false)}
      expandableRowExpanded={() => expendableRowShow}
    />
  );
  return <div className="page-container"> 
   <button
        ref={searchButtonRef} // Assign the ref to the search button
        className="search-button"
        type="submit"
        onClick={func_search}
      >
        Search <RxMagnifyingGlass className="search-icon" />
      </button>
  {datables}</div>;
};

export default MailTagCorrection;
