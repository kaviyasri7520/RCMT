import React, { useState, useEffect } from "react";
import "./UserInformation.css";
import useReactFontLoader from "react-font-loader";
import { RxMagnifyingGlass } from "react-icons/rx";
import axios from "axios";
import BtnLoadingSpinner from "../../components/loadingSpinner/BtnLoadingSpinner";
import Select from "react-select";
import { IoClose } from "react-icons/io5";
import { apiBaseURL } from "../../core/utils";

const UserInformation = () => {
  const [syenappIdList, setSyenappIdList] = useState([]);
  const [syenappId, setSyenappId] = useState("");
  const [pending, setPending] = useState(false);
  const [UserInformation, setUserInformation] = useState([]);
  const [afterSubmit, setAfterSubmit] = useState(false);

  useReactFontLoader({
    url: "https://fonts.googleapis.com/css2?family=Red+Hat+Display:wght@700&display=swap",
  });
  useReactFontLoader({
    url: "https://fonts.googleapis.com/css2?family=Poppins:wght@100;400&display=swap",
  });

  // useEffect for get syenappId list for dropdown
  useEffect(() => {
    axios
      .get(`https://${apiBaseURL()}/v2/rcmtcontroller/syenappidList`)
      .then((res) => {
        const result = res.data;
        let idList = result.map((e) => ({
          label: e.syenappid,
          value: e.syenappid,
        }));
        setSyenappIdList(idList);
      })
      .catch((err) => {
        console.log(err);
        alert("connot get syenappId list");
      });
  }, []);

  // selct syenappId from drop down
  const selectSyenappId = (event) => {
    setSyenappId(event.value);
    setAfterSubmit(false);
  };

  const onSearch = () => {
    if (syenappId !== "") {
      setPending(true);
      console.log(syenappId);
      axios
        .get(
          `https://${apiBaseURL()}/userauth/getuserinformation?mailId=${syenappId}`
        )
        .then((res) => {
          const result = res.data.data[0] || [];
          setUserInformation(result);
          console.log(res.data);
          setPending(false);
          setAfterSubmit(true);
        })
        .catch((err) => {
          console.log(err);
          alert(err.message);
          setPending(false);
        });
    } else {
      alert("Please enter the SyenappId");
    }
  };

  const onClear = () => {
    setSyenappId("");
    setUserInformation({});
    setAfterSubmit(false);
  };

  return (
    <div className="page-container">
      <div className="user-information-container">
        <div className="user-search-form">
          <Select
            className="syenapp-id-select"
            placeholder={"Select Syenapp Id"}
            value={{ label: syenappId, value: syenappId }}
            options={syenappIdList}
            onChange={selectSyenappId}
          />
          <IoClose title="clear" onClick={onClear} className="clear-icon" />
          <button
            type="submit"
            title="search"
            onClick={onSearch}
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
        </div>
        {pending && <h1>Loading...</h1>}
        {afterSubmit && UserInformation.length !== 0 && !pending && (
          <div className="user-information-card">
            <div style={{ fontFamily: "Red Hat Display" }} className="title">
              User Information
            </div>
            <table className="user-information-table">
              <tbody>
                <tr className="user-information-row">
                  <td>
                    <label>Mobile Number</label>
                  </td>
                  <td>{UserInformation.phone ? UserInformation.phone : "-"}</td>
                </tr>
                {/* <tr className='user-information-row'>
            <td><label>Syenapp Id</label></td>
            <td>{UserInformation.syenappid?UserInformation.syenappid:'-'}</td>
          </tr> */}
                <tr className="user-information-row">
                  <td>
                    <label>First Name</label>
                  </td>
                  <td>
                    {UserInformation.firstName
                      ? UserInformation.firstName
                      : "-"}
                  </td>
                </tr>
                <tr className="user-information-row">
                  <td>
                    <label>Last Name</label>
                  </td>
                  <td>
                    {UserInformation.lastName ? UserInformation.lastName : "-"}
                  </td>
                </tr>
                <tr className="user-information-row">
                  <td>
                    <label>Gender</label>
                  </td>
                  <td>
                    {UserInformation.gender ? UserInformation.gender : "-"}
                  </td>
                </tr>
                <tr className="user-information-row">
                  <td>
                    <label>Date of Birth</label>
                  </td>
                  <td>{UserInformation.DOB ? UserInformation.DOB : "-"}</td>
                </tr>
                <tr className="user-information-row">
                  <td>
                    <label>Country</label>
                  </td>
                  <td>
                    {UserInformation.country ? UserInformation.country : "-"}
                  </td>
                </tr>
                <tr className="user-information-row">
                  <td>
                    <label>Shipping Address</label>
                  </td>
                  <td>
                    {UserInformation.shippingAddress
                      ? UserInformation.shippingAddress
                      : "-"}
                  </td>
                </tr>
                <tr className="user-information-row">
                  <td>
                    <label>City</label>
                  </td>
                  <td>{UserInformation.city ? UserInformation.city : "-"}</td>
                </tr>
                <tr className="user-information-row">
                  <td>
                    <label>State</label>
                  </td>
                  <td>{UserInformation.state ? UserInformation.state : "-"}</td>
                </tr>
                <tr className="user-information-row">
                  <td>
                    <label>Created Date</label>
                  </td>
                  <td>{UserInformation.dateCreated}</td>
                </tr>
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

export default UserInformation;
