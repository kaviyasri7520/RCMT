import React,{useEffect, useState, useMemo} from "react";
import FilterComponent from "../../components/filter/FilterComponents";
import axios from 'axios';
import DataTable from "react-data-table-component";
import Modal from '../../components/popUpModals/modal';
import './AmazonUsers.css'
import HideTestUsers from "../../components/hideTestUsers/HideTestUsers";
import { apiBaseURL } from "../../core/utils";
const { apiStoreHeader, apiOcrHeader, apiElasticHeader,testUsersURL  } = require('../../config/config.json')

const AmazonUsers= ()=> {
    const [filterText, setFilterText] = React.useState("");
    const [resetPaginationToggle, setResetPaginationToggle] = React.useState(false);
    const [users,setUsers]=useState([]);
    const [show,setShow]=useState(false);
    const [syenappId,setSyenappid]= useState('')
    const[track,setTrack]=useState('');
    const [dynamicRewardPoints,setDynamicRewardPoints] = useState('') 
    const [pending, setPending] = useState(true);
    const [testUsersList,setTestUsersList] =  useState([])
    const [totalUsers,setTotalUsers] = useState([])
    const [currentCountry] = useState(localStorage.getItem("selectedCountry"))
  
    useEffect(() => {
      axios.get(`https://${apiBaseURL()}/v2/rewards/getconfigrewardpoints`)
      .then((res) => {
        const data = res.data.data[0]
        setDynamicRewardPoints(data.amazonSignUp)
      })
      .catch((err) => {console.log(err);alert('Cannot fetch the reward points')})
    }, []);

    // fetch s=amazon sign up user list
    useEffect(() => { 
      axios({
        url: `https://${apiBaseURL()}/v2/rcmtcontroller/amzuserlist`,
        method: 'get',
        headers: apiStoreHeader
      })
      .then((res)=>{
        const data=res.data.data;
      setUsers(data)
      setTotalUsers(data)
      setPending(false);
        }).catch((error)=> {console.log(error);alert('connection failed')})
    // get list of test users
      axios.get(`${testUsersURL}/testusers-list`)
        .then((res) => {
        let result = res.data
        setTestUsersList(result)
      })
      .catch((err) => {console.log(err)})
      document.getElementById("hidetestusers").checked = false ;
 },[track])



const handleClick =(syenappId)=> {

    setSyenappid(syenappId);
    showconfirmationPopup(syenappId);
    
   }

 const handleSubmit = ()=> {
  
    const objParams = {
         params: {
                   "syenappId": syenappId,
                   "key": "AMAZONSIGNUP",
                   "rewardPoints": dynamicRewardPoints
       }}
       debugger;
    if(dynamicRewardPoints !== ''){
      const url = currentCountry === 'us'
      ? `http://34.93.254.75:7128/user_input?userId=${syenappId}`
      : `http://34.93.254.75:7128/user_input?userId=${syenappId}`
      axios.get(url
         )
    .then((resp)=>{
   debugger;
      if(resp){
    axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/updateamzsignuprwdpts`, {
        syenappId: syenappId }, { headers: apiStoreHeader }  )
    .then((res)=>{
      setTrack(track+1);
      if(res){
        alert("Submitted Successfully");
        hideConfirmationPopup();
        axios.get(`https://${apiBaseURL()}/notification/sendnotificationtousers?mailId=${syenappId}&flag=AmazonSignUp`,
        {headers:apiOcrHeader})
        .then((res)=>{
        if(res){
            axios.put(`https://${apiBaseURL()}/elasearch/addusertask`,objParams,
            {headers: apiElasticHeader})
            .then((res)=>{
            })
            .catch((error)=> {console.log(error)})
          }})
            .catch((error)=> {console.log(error)})
          }
        })
   }
      
      })
        .catch((error)=> {console.log(error);alert('request failed')})
    } else{
      alert('Cannot fetch the reward points')
    }  
 }
 

 const showconfirmationPopup=()=>{
    // console.log(syenappId)
    setShow(true);
 }
 const hideConfirmationPopup=()=>{
    setShow(false);
 }

const confirmationPopup=(
    <Modal show={show} handleClose={hideConfirmationPopup}>
        <div className="empty"></div>
        <h2 className="title">Do you want to submit?</h2>
        <button onClick={hideConfirmationPopup} className="confirm">Cancel</button>
        <button onClick={handleSubmit} className="cancel">Confirm</button>
    </Modal>
)


    const columns = [
        {
            name: <div>SyenApp ID</div>,
            selector: row =>row.syenappId,
            sortable:true,
            center:"true"
        },
        {
            name: <div>First Name</div>,
            selector: row =>row.firstName,
            center:"true"
        },
        {
            name: <div>Mail Received Date</div>,
            selector: row =>new Date(row.amzLastReceivedMailDate).toString().substr(4, 12),
            sortable:true,
            center:"true"
        },
        {
            name: <div>Last Mail Subject</div>,
            selector: row =>row.amzLastReceivedMail,
            center:"true"
        },
        {
            name: <div>Mail Count</div>,
            selector: row =>row.amzMailCount,
            center:"true"
        },
        {
            name:<div></div>,
           
            selector:row =><div   style={{ fontSize: "15px ", fontFamily:"sans-serif",color:'black'}}  ><button className="view-address-btn"   onClick={() => handleClick(row.syenappId, row.kumulosToken)} >SUBMIT</button> {(row.button)}</div>,
            center:"true"
        }

    ];
//disabled={currentCountry ==='in' }
    // const ExpandedComponent = ({data}) => <pre  style={{ fontWeight: 400 , fontFamily:'sans-serif',border:'0px solid grey',color:'white' }}>  Amount : {(data.amount)}     Apply Date : {(data.applyDate)}        Payment Type : {(data.paymentType)}     Source : {(data.source)}      Refferal points : {(data.refferalRedeemPts)}     Subscription Points : {(data.subscribedRedeemPts)}    Other Points :{(data.otherRedeemPts)}   </pre>;

    const customStyles = {

      noData: {
         style: {
              marginTop: '16%',
              color: '#3b7acc',
              fontSize: '30px',
          },
      },
      headRow: {
          style: {
              color:'white',
              fontSize:'15px',
              backgroundColor: '#333222',
              minHeight: '52px',
              borderBottomWidth: '1px',
              borderBottomStyle: 'solid',
          },
          denseStyle: {
                minHeight: '32px',
          },
      },
  }
      //function for show & hide the test users
      const func_hideshowTestUser = () => {
       
        let currentState = document.getElementById("hidetestusers").checked;
        console.log(currentState)
        if(currentState){
          let testUsersRemovedData = users.filter(({syenappId:id1}) => (!testUsersList.some(({syenappId:id2}) => id2 === id1)))
          setUsers(testUsersRemovedData)
        }
        if(!currentState){
          setUsers(totalUsers)
        }
      }
    
      //checkbox component for show & hide the test users
      const subHeaderComponent1 = (
        <HideTestUsers handleChange={func_hideshowTestUser} checkboxId={'hidetestusers'} />
      )

      //filter
      const filteredItems = users.filter(
        item =>
          JSON.stringify(item.syenappId)
            .toLowerCase()
            .indexOf(filterText.toLowerCase()) !== -1
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
            onFilter={e => setFilterText(e.target.value)}
            onClear={handleClear}
            filterText={filterText}
          />
        );
      }, [filterText, resetPaginationToggle]);


    const dtable=(
    <DataTable
    columns ={columns}
    data={filteredItems}
    noDataComponent={"No records found"}
    customStyles={customStyles}
    pagination
    progressPending={pending}
    paginationPerPage = {25}
    paginationRowsPerPageOptions = {[25,50,75,100]}
    paginationComponentOptions={{
        rowsPerPageText: 'Records per page:',
        rangeSeparatorText: 'out of',
      }}
    subHeader
    subHeaderComponent={[subHeaderComponent1,subHeaderComponent]}
    />
    )
return(

    <div className="page-container">
      {dtable}
      {confirmationPopup}
    </div>

)

}

export default AmazonUsers;