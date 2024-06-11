import axios from "axios";
import React, { useState } from "react";
import "./MailSender.css";
import { apiBaseURL } from "../../core/utils";

const { apiOcrHeader } = require("../../config/config.json");

const MailSender = () => {
  const foot = `Thanks,
SyenApp support team`;

  const [mailId, setEmail] = useState("");
  const [subject, setSubject] = useState("");
  const [mailBody, setMailBody] = useState();
  // const [mailBodyHeader,setMailBodyHeader] = useState()
  const [mailBodyFooter, setMailBodyFooter] = useState(foot);
  const [showcampaign, setShowCampaign] = useState(false);
  const [campaignName, setCampaignName] = useState("");
  const [campaignURL, setCampaignURL] = useState("");

  let contents = [
    {
      title: "General Clarification & Auto Mail",
      content: `We appreciate your patience and will be happy to solve your issue. Our SyenApp support team will resolve your queries in the next 24 hours. Thank you for reaching out.`,
    },
    {
      title: "Missing Payments",
      content: `Thank you for your valuable time. We certainly understand your concern. We are sorry to hear that you're having issues with your reward redemption. Our support team is working on your issue. We will get back to you via email with a resolution within 1-2 business days.`,
    },
    {
      title: "SyenApp Referrals Points",
      content: `Thank you for your valuable time. We certainly understand your concern. Our support team will resolve and get back to you via an email response within 1-2 business days.`,
    },
    {
      title: "Amazon Reward Points",
      content: `Thank you for your valuable time. We are sorry to hear that you’re seeing a delay in your reward points for changing your login in Amazon, Our SyenApp support team will resolve your queries in 1-2 business days and shall respond in an email.`,
    },
  ];

  let colorsPalette = [
    "#9DE7CD",
    "#FDE9A2",
    "#F2C09C",
    "#FFC7B5",
    "#FFC9D5",
    "#C2DEE7",
    "#FFDFE6",
    "#CDF1FF",
    "#DCDEFF",
    "#E6F6FC",
  ];

  const input_handle = (event) => {
    let name = event.target.name;
    name === "mailContent" && setMailBody(event.target.value);

    name === "mailBodyFooter" && setMailBodyFooter(event.target.value);
    name === "mailId" && setEmail(event.target.value);
    name === "subject" && setSubject(event.target.value);
    name === "camp-name" && setCampaignName(event.target.value);
    name === "camp-url" && setCampaignURL(event.target.value);
  };

  const func_removeCampaign = () => {
    setShowCampaign(false);
    setCampaignName("");
    setCampaignURL("");
  };

  const sendMail = (event) => {
    event.preventDefault();

    const param = {
      mailId: mailId,
      subject: subject,
      body: mailBody,
      footer: mailBodyFooter,
      campaign: campaignName,
      link: campaignURL,
    };
    console.log(param);
    axios
      .put(`https://${apiBaseURL()}/common/rcmtsentmail`, param, {
        headers: apiOcrHeader,
      })
      .then((res) => {
        console.log(res.data);
        setShowCampaign(false);
        if (res) {
          alert("Mail sent Successfully");
          setSubject("");
          setMailBody("");
          setCampaignName("");
          setCampaignURL("");
          axios({
                  url: `https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${mailId}&flag=SUPPORTEMAIL`,
                  method: "get",
                  headers: apiOcrHeader,
                  })
            .then((res) => {
              if (res) {
                setEmail("");
              }
            })
            .catch((err) => {
              console.log(err);
              setEmail("");
            });
        }
      })
      .catch((error) => {
        console.log(error);
        alert("Failed to send");
      });
  };

  // mail body palette board
  const Palette = ({ preDefinedBody }) => {
    const get_text = (event) => {
      setMailBody(event.target.id);
    };

    return (
      Array.isArray(preDefinedBody) &&
      preDefinedBody.map((e, i) => {
        return (
          <div
            title="click to select a mail content"
            style={{ backgroundColor: colorsPalette[i] }}
            className="body-container"
            id={e.content}
            onClick={get_text}
          >
            {e.title}
          </div>
        );
      })
    );
  };

  const mailForm = (
    <form onSubmit={sendMail} className="mail-form">
      <fieldset className="mailform-fieldset">
        <label>To</label>
        <input
          type="email"
          className="mail-input"
          name="mailId"
          onChange={input_handle}
          value={mailId}
          required
        />

        <label>Subject</label>
        <textarea
          name="subject"
          id=""
          cols="80"
          rows="4"
          onChange={input_handle}
          value={subject}
          required
        ></textarea>

        <label>Mail Content</label>
        <textarea
          name="mailContent"
          id=""
          cols="80"
          rows="6"
          onChange={input_handle}
          value={mailBody}
          required
        ></textarea>

        <label>Mail Footer</label>
        <textarea
          name="mailBodyFooter"
          id=""
          cols="80"
          rows="3"
          onChange={input_handle}
          value={mailBodyFooter}
        ></textarea>

        {!showcampaign && (
          <button
            type="button"
            className="campaign-button"
            onClick={() => setShowCampaign(true)}
          >
            Add Campaign
          </button>
        )}

        {showcampaign && (
          <>
            <label>Campaign Name</label>
            <input
              type="text"
              className="mail-input"
              name="camp-name"
              onChange={input_handle}
              value={campaignName}
            />
            <label>Campaign URL</label>
            <input
              type="text"
              className="mail-input"
              name="camp-url"
              onChange={input_handle}
              value={campaignURL}
            />{" "}
          </>
        )}

        {showcampaign && (
          <button
            type="button"
            className="campaign-cancel-button"
            onClick={func_removeCampaign}
          >
            Cancel
          </button>
        )}
        <button type="submit" className="send-button">
          Send Mail
        </button>
      </fieldset>
    </form>
  );

  const mailTemplates = (
    <div className="template-board">
      <Palette preDefinedBody={contents} />
    </div>
  );

  return (
    <div className="page-container">
      <span className="mail-container">
        {mailForm}
        {mailTemplates}
      </span>
    </div>
  );
};

export default MailSender;
