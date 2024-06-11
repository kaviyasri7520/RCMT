import React, { useState } from 'react';
import axios from "axios";
import './ReferralPoints.css'
import { apiBaseURL } from "../../core/utils";

function Refcode() {
  const [SyenappReferralCode, setSyenappReferralCode] = useState("");
  const [syenappId, setsyenappID] = useState("");
  const [totalRwdPoints, setTotalRewardPoints] = useState("");
  const [totalReferrals, setTotalReferrals] = useState("");
  const [totalPendingReferralPts, setTotalPendingReferralPoints] = useState("");
  const [totalClaimedReferralPts, setTotalClaimedReferralPoints] = useState("");
  const [totalRejectedReferralPts, setTotalRejectedReferralPoints] = useState("");
  const [totalRedeemReferralPts, setTotalRedeemReferralPoints] = useState("");
  const [totalOtherRedeemedPts, setTotalOtherRedeemedPoints] = useState("");
  const [totalOtherClaimedPts, setTotalOtherClaimedPoints] = useState("");
  const [totalOtherPendingPoints, setTotalOtherPendingPoints] = useState("");
  const [totalOtherRejectedPoints, setTotalOtherRejectedPoints] = useState("");
  const [totalPendingPoints, setTotalPendingPoints] = useState("");
  const [totalRedeemedPoints, setTotalRedeemedPoints] = useState("");
  const [totalClaimedPoints, setTotalClaimedPoints] = useState("");

  const handleInputChange = (event) => {
    setSyenappReferralCode(event.target.value);
  };

  const handleSubmit = async (event) => {
    event.preventDefault();
    try {
      // const response = await axios.post(`https://${apiBaseURL()}/v2/rcmtcontroller/fetchuserredeeminfo?refcode=${SyenappReferralCode}`);
      const response = await axios.get(`https://${apiBaseURL()}/v2/rcmtcontroller/fetchuserredeeminfo?refcode=${SyenappReferralCode}`);
      const data = response.data.data[0];
      setsyenappID(data.syenappId);
      setTotalRewardPoints(data.totalRwdPoints);
      setTotalReferrals(data.totalReferrals);
      setTotalPendingReferralPoints(data.totalPendingReferralPts);
      setTotalClaimedReferralPoints(data.totalClaimedReferralPts);
      setTotalRejectedReferralPoints(data.totalRejectedReferralPts);
      setTotalRedeemReferralPoints(data.totalRedeemReferralPts);
      setTotalOtherRedeemedPoints(data.totalOtherRedeemedPts);
      setTotalOtherClaimedPoints(data.totalOtherClaimedPts);
      setTotalOtherPendingPoints(data.totalOtherPendingPoints);
      setTotalOtherRejectedPoints(data.totalOtherRejectedPoints);
      setTotalPendingPoints(data.totalPendingPoints);
      setTotalRedeemedPoints(data.totalRedeemedPoints);
      setTotalClaimedPoints(data.totalClaimedPoints);
      // setSyenappReferralCode(""); 
    } catch (error) {
      alert("No SyenappID found");
    }
  };

  return (
    <div>
      <div className="container">
        <form onSubmit={handleSubmit} className="head">
          <p className="refcode">Enter Refcode:</p>
          <input
            type="text"
            value={SyenappReferralCode}
            onChange={handleInputChange}
            className="input-id"
            placeholder="Enter referral code"
          />
          <input
            type="submit"
            value="Submit"
            className="submit-button"
          />
        </form>
      </div>

      <div className="results-header">
        <label className="results" htmlFor="syenappId">Syenapp ID:</label>
        <label className="input-id">{syenappId}</label>

        <label className="results" htmlFor="totalRwdPoints">Total Reward Points:</label>
        <label className="input">{totalRwdPoints}</label>
      </div>

      <div className="sub-results">
        <div className="output-row"> 
        <label className="results" htmlFor="totalReferrals">Total Referrals:</label>
        <label className="input">{totalReferrals}</label>
        </div>

        <div className="referral-row">
        <label className="results" htmlFor="totalPendingReferralPts">Pending Referral Points:</label>
        <label className="input">{totalPendingReferralPts}</label>
        </div>

        <div className="referral-row">
        <label className="results" htmlFor="totalClaimedReferralPts">Claimed Referral Points:</label>
        <label className="input">{totalClaimedReferralPts}</label>
        </div>

        <div className="referral-row">
        <label className="results" htmlFor="totalRejectedReferralPts">Rejected Referral Points:</label>
        <label className="input">{totalRejectedReferralPts}</label>
        </div>

        <div className="referral-row">
        <label className="results" htmlFor="totalRedeemReferralPts">Redeemed Referral Points:</label>
        <label className="input">{totalRedeemReferralPts}</label>
        </div>

        <div className="other-row">
        <label className="results" htmlFor="totalOtherRedeemedPts">Other Redeemed Points:</label>
        <label className="input">{totalOtherRedeemedPts}</label>
        </div>

        <div className="other-row">
        <label className="results" htmlFor="totalOtherClaimedPts">Other Claimed Points:</label>
        <label className="input">{totalOtherClaimedPts}</label>
        </div>

        <div className="other-row">
        <label className="results" htmlFor="totalOtherPendingPoints">Other Pending Points:</label>
        <label className="input">{totalOtherPendingPoints}</label>
        </div>

        <div className="other-row">
        <label className="results" htmlFor="totalOtherRejectedPoints">Other Rejected Points:</label>
        <label className="input">{totalOtherRejectedPoints}</label>
        </div>
      </div>
      <div className="results-footer">
      <label className="results" htmlFor="totalPendingPoints">Total Pending Points:</label>
      <label className="input">{totalPendingPoints}</label>

      <label className="results" htmlFor="totalRedeemedPoints">Total Redeemed Points:</label>
      <label className="input">{totalRedeemedPoints}</label>
      
      <label className="results " htmlFor="totalClaimedPoints">Total Claimed Points:</label>
      <label className="input">{totalClaimedPoints}</label>
      </div>

    </div>
  );
}

export default Refcode;
