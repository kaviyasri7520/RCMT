import React, { useEffect, useState, useMemo } from "react";
import "./DealsView.css";
import { DatalistInput } from "react-datalist-input";
import DataTable from "react-data-table-component";
import FilterComponent from "../../components/filter/FilterComponents";
import axios from "axios";
import { RxMagnifyingGlass } from "react-icons/rx";
import BtnLoadingSpinner from "../../components/loadingSpinner/BtnLoadingSpinner";
import { MdContentCopy } from "react-icons/md";
import { RxReset } from "react-icons/rx";
import copy from "clipboard-copy";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import { apiBaseURL } from "../../core/utils";

const DealsView = () => {
  const [filterText, setFilterText] = useState("");
  const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
  const [currentCountry] = useState(localStorage.getItem("selectedCountry"))
  const options1 = [
    { id: "Buy 1 Get 1", value: "Buy 1 Get 1" },
    { id: "Free Shipping", value: "Free Shipping" },
    { id: "Coupons", value: "Coupons" },
    { id: "%", value: "Percentage" },
  ];
  const [primaryCategory, setprimaryCategory] = useState("");
  const [country, setCountry] = useState("");
  const [searchText, setSearchText] = useState("");
  const [categoryList, setcategoryList] = useState([
  ""
  ]);
  const [pending, setPending] = useState(false);
  const [dealsDetails, setDealsDetails] = useState([]);

  const [expendableRowShow,setExpandableRowShow] = useState(false)

  useEffect(() => {
    axios
      .get(`https://${apiBaseURL()}/v2/rcmtcontroller/getcategoryname`)
      .then((res) => {
        const result = res.data;
        // console.log(result)
        setcategoryList(result);
        // setCategoryId(result[0].categoryId)
        // setCategoryName(result[0].categoryName)
      })
      .catch((err) => {
        console.log(err);
        alert("request failed");
      });
  }, []);

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
        backgroundColor: "#0056b3",
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
        // width:'2rem ! important',
        color: "#F5F6F6",
        textDecoration: "none",
        fontSize: "16px",
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

  const columns = [
    {
      name: <div>Deal Description</div>,
      selector: (row) => row.dealsDesc,
      center: "true",
    },
    {
      name: <div>Start Date</div>,
      selector: (row) => {
        if (row.dtStartDate) {
          const dateWithoutTime = row.dtStartDate.split('T')[0];
          return new Date(dateWithoutTime).toLocaleDateString('en-GB');
        } else {
          return '';
        }
      },
      sortable:true,
      center: "true",
    },
    {
      name: <div>End Date</div>,
      selector: (row) => {
        if (row.dtEndDate) {
          const dateWithoutTime = row.dtEndDate.split('T')[0];
          return new Date(dateWithoutTime).toLocaleDateString('en-GB');
        } else {
          return '';
        }
      },
      sortable:true,
      center: "true",
    },
    {
      name: <div>Coupon ID</div>,
      selector: (row) => row.couponId,
      center: "true",
    },
    {
      name: <div>Merchant Name</div>,
      selector: (row) => row.merchantName,
      center: "true",
    },
    {
      name: <div>Merchant ID</div>,
      selector: (row) => row.merchantId,
      center: "true",
    },
    {
      name: <div>Special Deals</div>,
      selector: (row) => row.specialDeals,
      center: "true",
      sortable:true
    },
  ];

  const ExpandedComponent = ({ data }) => (
    <div className="extendable-box">
      <div className="extendable-label">
        &nbsp;&nbsp;&nbsp;&nbsp;Deals
        Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        -&nbsp;&nbsp;&nbsp;&nbsp;<div>{data.dealsDesc}</div>
      </div>
      <div className="extendable-label">
        &nbsp;&nbsp;&nbsp;&nbsp;FMTC
        URL&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        - &nbsp;&nbsp;&nbsp;&nbsp;
        <a
          target="_blank"
          id={data.FMTCURL}
          href={data.FMTCURL}
          rel="noreferrer"
          className="extendable-url"
        >
          {data.FMTCURL}
        </a>
        <MdContentCopy
          onClick={() => copyURL(data)}
          title="copy URL"
          className="copy-icon"
        />
      </div>
      <div className="extendable-label">
        &nbsp;&nbsp;&nbsp;&nbsp;Country&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        -&nbsp;&nbsp;&nbsp;&nbsp;<div>{data.country}</div>
      </div>
      <div className="extendable-label">
        &nbsp;&nbsp;&nbsp;&nbsp;Ships To Countries&nbsp;&nbsp;&nbsp;&nbsp;
        -&nbsp;&nbsp;&nbsp;&nbsp;<div>{data.shipsToCountries}</div>
      </div>
    </div>
  );

  const copyURL = (data) => {
    if (data.FMTCURL !== "") {
      copy(data.FMTCURL)
        .then(() => {
          console.log(data.FMTCURL);
          toast.success("URL copied!", { autoClose: 1000 });
        })
        .catch((error) => {
          alert("Failed to write to clipboard:", error);
        });
    } else {
      toast.error("URL is not copied");
      console.log("failed", data.FMTCURL);
    }
  };

  const inputHandle = ({ target }) => {
    if (target.name === "primaryCategory") {
      setprimaryCategory(target.value);
    } else if (target.name === "country") {
      setCountry(target.value);
    } else {
      setSearchText(target.value);
    }
  };

  const searchSubmit = (e) => {
    e.preventDefault();
    setPending(true);
    const params = {
      country: country,
      primaryCategory: primaryCategory,
      searchText: searchText.trim(),
    };
    axios
      .put(`https://${apiBaseURL()}/v2/rcmtcontroller/getdealsbycategoryid`, params)
      .then((res) => {
        const result = res.data;
        // console.log(res)
        setPending(false);
        if(result.err){
          toast.error("Failed to fetch data");          
        }
        else{
          setDealsDetails(result);
        }       
      })
      .catch((err) => {
        console.log(err);
        alert("request failed");
        setPending(false);
      });
  };

  const func_reset = () => {
    setprimaryCategory("");
    setCountry("");
    setSearchText("");
    setDealsDetails([]);
  };

  //filter
  const filteredItems = dealsDetails.filter(
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
      <div className="filter-input-box">
        <label>Find In Results : </label>
        <FilterComponent
          onFilter={(e) => setFilterText(e.target.value)}
          onClear={handleClear}
          filterText={filterText}
        />{" "}
      </div>
    );
  }, [filterText, resetPaginationToggle]);
  

  return (
    <div className="deals-container">
      <form className="deals-input-form">
        <label>&nbsp;&nbsp;Select Country :</label>
        {currentCountry === "us" && <select
          style={{ width: "4rem" }}
          name="country"
          placeholder="Select Category"
          value={country}
          onChange={inputHandle}
        >
          <option style={{ display: "none" }} value=""></option>
          <option value="us">US</option>
          <option value="ca">CA</option>
        </select>}
        {currentCountry === "in" && <select
          style={{ width: "4rem" }}
          name="country"
          placeholder="Select Category"
          value="in"
          //onChange={inputHandle}
        >
          <option style={{ display: "none" }} value=""></option>
          <option value="in">India</option>
        </select>}
        <label>Select primaryCategory :</label>
        <select
          name="primaryCategory"
          placeholder="Select Category"
          onChange={inputHandle}
        >
          <option style={{ display: "none" }} value=""></option>
          {Array.isArray(categoryList) &&
            categoryList.map((e) => (
              <option key={e.primaryCategory} value={e.primaryCategory}>
        {e.primaryCategory}
      </option>
            ))}
        </select>
        <DatalistInput
          className="keyword-input"
          placeholder="Enter keyword in Description"
          autoComplete="off"
          items={options1}
          value={searchText}
          setValue={setSearchText}
          onChange={inputHandle}
          onselect={inputHandle}
        />
        <button
          type="submit"
          onClick={searchSubmit}
          className="submit-button"
          disabled={pending}
        >
          {pending ? (
            <BtnLoadingSpinner />
          ) : (
            <>
              Search
              <RxMagnifyingGlass className="search-icon" />
            </>
          )}
        </button>
        <button
          type="reset"
          onClick={func_reset}
          className="submit-button"
          disabled={pending}
        >
          Reset
          <RxReset className="search-icon" />
        </button>
      </form>
      <DataTable
        columns={columns}
        customStyles={customStyles}
        data={filteredItems}
        progressPending={pending}
        noDataComponent={"No records found"}
        subHeader
        subHeaderComponent={[subHeaderComponent]}
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
      <ToastContainer toastClassName={"custom-toast"} />
    </div>
  );
};

export default DealsView;
