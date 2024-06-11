import React, { useState, useEffect, useMemo } from "react";
import "./SearchKeyWord.css";
import DataTable from "react-data-table-component";
import { AiFillEdit } from "react-icons/ai";
import { ImBin2 } from "react-icons/im";
import axios from "axios";
import BtnLoadingSpinner from "../../components/loadingSpinner/BtnLoadingSpinner";
import Modal from "../../components/popUpModals/modal";
import FilterComponent from "../../components/filter/FilterComponents";
import { RxReset } from "react-icons/rx";
import { apiBaseURL } from "../../core/utils";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

const { apiElasticHeader } = require("../../config/config.json");

const SearchKeyWord = () => {
  const [filterText, setFilterText] = useState("");
  const [resetPaginationToggle, setResetPaginationToggle] = useState(false);

  const [pending, setPending] = useState(false);
  const [submitPending, setSubmitPending] = useState(false);
  const [params, setParams] = useState({
    textSearch: "",
    textDisplay: "",
    flag: "",
    seqNo: "",
    shipsToCountries: "",
    isActive: false,
  });
  const [flagOptions, setFlagOptions] = useState([]);
  const [searchData, setSearchData] = useState([]);
  const [editActive, setEditActive] = useState(false);
  const [sQuery, setsQuery] = useState("");
  const [addTrack, setAddTrack] = useState("0");
  const [deleteTrack, setDeleteTrack] = useState(1);
  const [show, setShow] = useState(false);
  const [deleteTextKey, setDeleteTextKey] = useState("");
  const [tempSeqNo, setTempSeqNo] = useState("");
  const [filterFlag, setFilterFlag] = useState("");
  const [expendableRowShow,setExpandableRowShow] = useState(false)

  useEffect(() => {
    axios(`https://${apiBaseURL()}/textsearch/gettextsearchflag`, {
      method: "get",
      headers: apiElasticHeader,
    })
      .then((res) => {
        let data = res.data.data;
        setFlagOptions(data);
        setParams((e) => ({ ...e, flag: data[0] }));
      })
      .catch((err) => console.log(err));
  }, []);

  useEffect(() => {
    setPending(true);
    setFilterFlag("");
    const timeoutId = setTimeout(async () => {
      await axios(
        `https://${apiBaseURL()}/textsearch/fetchtextsearch?pageNum=${0}&sQuery=${sQuery}`,
        {
          method: "get",
          headers: apiElasticHeader,
        }
      )
        .then((res) => {
          // console.log(res.data)
          setSearchData(res.data.data.listItems || res.data.data);
          setPending(false);
        })
        .catch((err) => {
          console.log(err);
          setPending(false);
          alert("Cannot get data");
        });
    }, 1100);

    return () => clearTimeout(timeoutId);
  }, [addTrack, deleteTrack]);

  useEffect(() => {
    if (params.flag !== "Stores") {
      setParams((prev) => ({ ...prev, seqNo: "" }));
    }
    if (params.flag === "Stores") {
      setParams((prev) => ({ ...prev, seqNo: tempSeqNo }));
    }
  }, [params.flag]);

  const customStyles = {
    noData: {
      style: {
        marginTop: "6%",
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
    rows: {
      style: {
        minHeight: "52px", // override the row height
        fontSize: "15px",
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
        paddingLeft: "10px",
        paddingRight: "10px",
        wordBreak: "break-word",
      },
    },
    subHeader: {
      style: {
        justifyContent: "flex-start",
        minHeight: "52px",
        border: "1px solid black",
        font: "bold 14px poppins",
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

  const handleInput = ({ target }) => {
    let name = target.name;
    if (name !== "seqNo") {
      const details = { ...params };
      details[name] = target.value;
      setParams(details);
    }
    if (name === "seqNo") {
      setTempSeqNo(target.value);
      setParams((prev) => ({
        ...prev,
        seqNo: target.value.replace(/[a-zA-Z]/g, "").replace(/[^\w\s]|_/gi, ""),
      }));
    }
  };

  const change_filterFlag = ({ target }) => {
    setFilterFlag(target.value);
    setPending(true);
    setsQuery("");
    axios(
      `https://${apiBaseURL()}/textsearch/fetchtextsearchbyflag?flag=${
        target.value
      }&pageNum=${0}`,
      {
        method: "get",
        headers: apiElasticHeader,
      }
    )
      .then((res) => {
        const result = res.data;
        setSearchData(result.data.listItems || res.data.data);
        setPending(false);
      })
      .catch((err) => {
        console.log(err);
        alert("request failed");
        setPending(false);
      });
  };
  const onCancel = () => {
    setParams({
      textSearch: "",
      textDisplay: "",
      flag: flagOptions[0],
      shipsToCountries: "",
      isActive: false,
    });
    document.getElementById("checkbox").checked = false;
    setEditActive(false);
  };

  const onEdit = (row) => {
    setParams(row);
    setTempSeqNo(row.seqNo || "");
    setSubmitPending(false);
    setEditActive(true);
    document.getElementById("checkbox").checked = row.isActive;
  };

  const hidePopup = () => {
    setShow(false);
  };

  const func_changeIsActive = () => {
    let l = document.getElementById("checkbox").checked;
    setParams((e) => ({ ...e, isActive: l }));
  };

  const onSearch = ({ target }) => {
    setFilterFlag("");
    setsQuery(target.value);
    let searchValue = target.value;
    setPending(true);
    axios(
      `https://${apiBaseURL()}/textsearch/fetchtextsearch?pageNum=${0}&sQuery=${searchValue}`,
      {
        method: "get",
        headers: apiElasticHeader,
      }
    )
      .then((res) => {
        setSearchData(res.data.data.listItems || res.data.data);
        setPending(false);
      })
      .catch((err) => {
        console.log(err);
        setPending(false);
        alert("Cannot get data");
      });
  };

  const func_reset = () => {
    setsQuery("");
    setFilterFlag("");
    setAddTrack(addTrack + "1");
  };

  const onSave = () => {
    Object.keys(params).map(
      (e) =>
        (params[e] =
          typeof params[e] === "string" ? params[e].trim() : params[e])
    );
    params.flag !== "Stores" && delete params["seqNo"];
    if (
      params.textSearch.trim() !== "" &&
      params.textDisplay.trim() !== "" &&
      params.flag !== ""
    ) {
      setSubmitPending(true);
      axios
        .put(`https://${apiBaseURL()}/textsearch/addmoretextsearch`, params, {
          headers: apiElasticHeader,
        })
        .then((res) => {
          res.data.status === 1 && toast.success("Added successfully")
          setAddTrack(addTrack + "1");
          setSubmitPending(false);
          res.data.status.includes("document already exists")
            ? alert("Search Display value is already exists")
            : setParams({
                textSearch: "",
                textDisplay: "",
                flag: flagOptions[0],
                seqNo: "",
                shipsToCountries: "",
                isActive: false,
              });
          !res.data.status.includes("document already exists") &&
            (document.getElementById("checkbox").checked = false);
        })
        .catch((err) => {
          console.log(err);
          setSubmitPending(false);
          toast.error("failed to add")
        });
    }
    params.textSearch.trim() === "" && toast.error("Please enter Text Search");
    params.textDisplay.trim() === "" && toast.error("Please enter Text Display");
    params.flag === "" && toast.error("Flag should not be empty");
  };

  const onUpdate = () => {
    Object.keys(params).map(
      (e) =>
        (params[e] =
          typeof params[e] === "string" ? params[e].trim() : params[e])
    );
    params.flag !== "Stores" && delete params["seqNo"];
    if (
      params.textSearch.trim() !== "" &&
      params.textDisplay.trim() !== "" &&
      params.flag !== ""
    ) {
      setSubmitPending(true);
      axios
        .put(`https://${apiBaseURL()}/textsearch/edittextsearch`, params, {
          headers: apiElasticHeader,
        })
        .then((res) => {
          const result = res.data
          result.status === 1 && toast.success("Updated successfully")
          axios
          .put(`https://${apiBaseURL()}/v2/rcmtcontroller/updatetextsearch`, params)
          .then((res) => {
          console.log(res)
          setPending(false);
          setSubmitPending(false);
          setParams({
            textSearch: "",
            textDisplay: "",
            flag: flagOptions[0],
            seqNo: "",
            shipsToCountries: "",
            isActive: false,
          });
          document.getElementById("checkbox").checked = false;
          setAddTrack(addTrack + "1");
          setEditActive(false);
        })
        }).catch((err) => {console.log(err);toast.error("failed to update in mongodb")})
        .catch((err) => {
          console.log(err);
          setSubmitPending(false);
          toast.error("failed to update")
        });
    }
    params.textSearch.trim() === "" && toast.error("Please enter Text Search");
    params.textDisplay.trim() === "" &&  toast.error("Please enter Text Display");
    params.flag === "" && toast.error("Flag should not be empty");
  };

  const onDelete = () => {
    setShow(false);
    axios(
      `https://${apiBaseURL()}/textsearch/removetextsearch?textKey=${deleteTextKey}`,
      {
        method: "get",
        headers: apiElasticHeader,
      }
    )
      .then((res) => {
        res.data.status === "Success" &&
          setDeleteTrack(parseInt(deleteTrack) + parseInt(1));
        res.data.status === "Success" &&
          console.log(deleteTrack, parseInt(deleteTrack) + parseInt(1));
        setDeleteTextKey("");
      })
      .catch((err) => {
        console.log(err);
        alert("request failed");
      });
  };

  const columns = [
    {
      name: <div>Text Search</div>,
      selector: (row) => row.textSearch,
      center: "true",
    },
    {
      name: <div>Text Display</div>,
      selector: (row) => row.textDisplay,
      center: "true",
    },
    {
      name: <div>Countries</div>,
      selector: (row) => row.shipsToCountries,
      center: "true",
    },
    {
      name: <div>Flag</div>,
      selector: (row) => row.flag,
      center: "true",
    },
    {
      name: <div>Seq Number</div>,
      selector: (row) => row.seqNo,
      center: "true",
    },
    {
      name: <div>isActive</div>,
      selector: (row) => JSON.stringify(row.isActive),
      center: "true",
    },
    {
      name: <div>Action</div>,
      button: true,
      cell: (row) => (
        <>
          <button
            className="mi-table-view-btn"
            onClick={() => onEdit(row)}
            style={{ marginRight: "5px", cursor: "pointer" }}
          >
            <AiFillEdit
              style={{ fontSize: "17px", color: "#3f6eae" }}
              className="mi-table-view-icon"
            />
          </button>
          <button
            className="mi-table-view-btn"
            onClick={() => {
              setDeleteTextKey(row.textKey);
              setShow(true);
            }}
            style={{ marginRight: "5px", cursor: "pointer" }}
          >
            <ImBin2
              style={{ fontSize: "17px", color: "red" }}
              className="mi-table-view-icon"
            />
          </button>
        </>
      ),
    },
  ];

  const ExpandedComponent = ({ data }) => (
    <table>
      <tbody>
        <tr>
          <th
            style={{
              fontSize: "17px",
              fontWeight: "bold",
              letterSpacing: "1px",
            }}
            className="thead"
          >
            Text Search
          </th>
        </tr>
        <tr>
          <td
            style={{
              fontSize: "17px",
              fontWeight: "bold",
              letterSpacing: ".5px",
            }}
            className="tdata"
          >
            {data.textSearch}
          </td>
        </tr>
      </tbody>
    </table>
  );

  //inner filter
  const filteredItems = searchData.filter(
    (item) =>
      JSON.stringify(item).toLowerCase().indexOf(filterText.toLowerCase()) !==
      -1
  );

  const subHeaderComponent3 = useMemo(() => {
    const handleClear = () => {
      if (filterText) {
        setResetPaginationToggle(!resetPaginationToggle);
        setFilterText("");
      }
    };

    return (
      <div className="inner-local-filter">
        <label>Find In Results </label>
        <FilterComponent
          onFilter={(e) => setFilterText(e.target.value)}
          onClear={handleClear}
          filterText={filterText}
        />
      </div>
    );
  }, [filterText, resetPaginationToggle]);

  const subHeaderComponent1 = (
    <div className="subheader-div">
      <label>Search by Flag </label>
      <select
        className="flag-filter-select"
        name="filter-flag"
        onChange={change_filterFlag}
        value={filterFlag}
      >
        <option selected style={{ display: "none" }} value=""></option>
        {flagOptions.map((e, i) => (
          <option key={i} value={e}>
            {e}
          </option>
        ))}
      </select>
      {/* <button style={{marginRight:'1rem',border:'1px solid black',height:'33px',fontWeight:'bold'}} className="clearbutton" onClick={() => {setFilterFlag('');setAddTrack(addTrack + '1')}}>X</button> */}
      <label>Search by Text Search </label>

      <input
        className="text-search-input"
        id="search"
        type="text"
        size={5}
        value={sQuery}
        onChange={onSearch}
        autoComplete="off"
      />

      <button type="reset" onClick={func_reset} className="submit-button">
        Reset
        <RxReset className="search-icon" />
      </button>
      {subHeaderComponent3}
    </div>
  );

  const confirmationDelete_popUp = (
    <Modal show={show} handleClose={hidePopup}>
      <h2 className="title">Do you want to Continue?</h2>
      <button onClick={() => setShow(false)} className="confirm">
        Cancel
      </button>
      <button onClick={onDelete} className="cancel">
        Yes,Delete
      </button>
    </Modal>
  );

  return (
    <div className="page-container">
      <table className="search-keyword-table">
        <tbody className="search-keyword-tbody">
          <tr>
            <td>
              <label>Text Search :</label>
            </td>
            <td colSpan={5}>
              <textarea
                name="textSearch"
                onChange={handleInput}
                value={params.textSearch}
                autoComplete="off"
              />
            </td>
          </tr>
          <tr>
            <td>
              <label>Flag :</label>
            </td>
            <td>
              <select name="flag" onChange={handleInput} value={params.flag}>
                {flagOptions.map((e, i) => (
                  <option key={i} value={e}>
                    {e}
                  </option>
                ))}
              </select>
            </td>
            <td>
              <label>Countries :</label>
            </td>
            <td>
              <input
                name="shipsToCountries"
                onChange={handleInput}
                value={params.shipsToCountries}
                autoComplete="off"
              />
            </td>
            <td>
              <label>Text Display :</label>
            </td>
            <td>
              <input
                name="textDisplay"
                onChange={handleInput}
                value={params.textDisplay}
                autoComplete="off"
              />
            </td>
          </tr>
          <tr>
            <td>
              <label>Seq Number :</label>
            </td>
            <td>
              <input
                name="seqNo"
                onChange={handleInput}
                value={params.seqNo}
                disabled={params.flag === "Stores" ? false : true}
              />
            </td>
            <td>
              <label>Is Active :</label>
            </td>
            <td>
              <input
                name="isActive"
                id="checkbox"
                onChange={func_changeIsActive}
                value={params.isActive}
                className="is-active-checkbox"
                type="checkbox"
              />
            </td>
            <td colSpan={2}>
              <button
                className="search-add-btn"
                onClick={editActive ? onUpdate : onSave}
              >
                {editActive ? (
                  submitPending ? (
                    <BtnLoadingSpinner />
                  ) : (
                    "Update"
                  )
                ) : submitPending ? (
                  <BtnLoadingSpinner />
                ) : (
                  "Add"
                )}
              </button>
              {editActive && (
                <button
                  className="search-cancel-btn"
                  style={
                    submitPending
                      ? { cursor: "not-allowed" }
                      : { cursor: "pointer" }
                  }
                  onClick={onCancel}
                  disabled={submitPending}
                >
                  Cancel
                </button>
              )}{" "}
            </td>
          </tr>
        </tbody>
      </table>
      <hr />
      <DataTable
        columns={columns}
        data={filteredItems}
        progressPending={pending}
        noDataComponent={"No records found"}
        customStyles={customStyles}
        subHeader
        subHeaderComponent={[subHeaderComponent1]}
        expandableRows
        expandableRowsComponent={ExpandedComponent}
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
      {confirmationDelete_popUp}
      <ToastContainer />
    </div>
  );
};

export default SearchKeyWord;
