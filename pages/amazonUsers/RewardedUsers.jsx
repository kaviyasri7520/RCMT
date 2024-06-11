import React,{useEffect, useState, useMemo} from "react";

import { apiBaseURL } from "../../core/utils";
import axios from 'axios';
import FilterComponent from "../../components/filter/FilterComponents";
import DataTable from "react-data-table-component";
import HideTestUsers from "../../components/hideTestUsers/HideTestUsers";
const { apiStoreHeader, testUsersURL } = require('../../config/config.json')



const RewardedUsers= ()=> {
    const [users,setUsers]=useState([]);
    const [filterText, setFilterText] = React.useState("");
    const [resetPaginationToggle, setResetPaginationToggle] = React.useState(false);

    const [pending, setPending] = React.useState(true);
    const [expendableRowShow,setExpandableRowShow] = useState(false)
    const [testUsersList,setTestUsersList] =  useState([])
    const [totalUsers,setTotalUsers] = useState([])

    React.useEffect(() => {
      const timeout = setTimeout(() => {
        // setRows(users);
        setPending(false);
      }, 2000);
      return () => clearTimeout(timeout);
    }, []);

    useEffect(() => {
        axios({
            url: `https://${apiBaseURL()}/v2/rcmtcontroller/claimedamzsignuprwdpts`,
            method: 'get',
            headers: apiStoreHeader
         })
        .then((res)=>{
        const data=res.data.data;  
        setUsers(data)
        setTotalUsers(data)
        }).catch((error)=> {console.log(error);alert('connection failed')})

      // get list of test users
      axios.get(`${testUsersURL}/testusers-list`)
        .then((res) => {
        let result = res.data
        setTestUsersList(result)
        })
        .catch((err) => {console.log(err)})
    document.getElementById("hidetestusers").checked = false ;
 },[])

 const customStyles = {
    noData: {
      style: {
          marginTop:'16%',
          color:'#3b7acc' ,
          fontSize: '30px',
      },
  
  },
  headRow: {
    style: {
        color:'white',
        fontSize:'16px',
        backgroundColor:' #333222',
        minHeight: '52px',
        borderBottomWidth: '1px',
        borderBottomStyle: 'solid',
    },
    denseStyle: {
       minHeight: '32px',
    },
  },
  rows: {
  style: {
      minHeight: '52px', // override the row height
      fontSize:'16px',
      textAlign: 'center',
      color:'black' ,
      
  },
  },
  headCells: {
    style: {
        color:'#F5F6F6',
        textDecoration:'none',
        fontSize:'17px',
        textAlign: 'center',
        justifyContent:'center',
        
    },
  },
  pagination: {
  style: {
    color:'black' ,
    fontSize: '13px',
    minHeight: '56px',
    fontWeight:"bolder",
    backgroundColor: 'white',
    borderTopStyle: 'solid',
    borderTopWidth: '1px',
    borderTopColor: '#3F6EAE',
  },
  pageButtonsStyle: {
    borderRadius: '50%',
    height: '40px',
    width: '40px',
    padding: '8px',
    cursor: 'pointer',
    transition: '0.4s',
    color: 'white',
    fill: 'black',
    backgroundColor: 'transparent',
    '&:hover:not(:disabled)': {
      backgroundColor: '#3b7acc',
      fill:'black'
    },
    '&:focus': {
      outline: 'none',
      backgroundColor: '#13243A',
      fill:'white'
    },
  },
  },
  }


    const columns = [
        {
            name: <div>SyenApp ID</div>,
            selector: row =>row.syenappId,
            sortable: true,
            center:"true"
        },
        {
            name: <div>First Name</div>,
            selector: row =>row.firstName,
            center:"true"
        },
        {
            name: <div>Reward Points</div>,
            selector: row =>row.rewardPoints,
            center:"true"
        },
        {
            name: <div>Reward Given Data</div>,
            selector: row => new Date(row.claimedDate).toString().substr(4, 12),
            sortable:true,
            center:"true"
        },
    ];

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
            onFilter={e => setFilterText(e.target.value)}
            onClear={handleClear}
            filterText={filterText}
          />
        );
      }, [filterText, resetPaginationToggle]);

return(
<DataTable
columns ={columns}
data={filteredItems}
noDataComponent={"No records found"}
customStyles={customStyles}
progressPending={pending}
pagination
paginationRowsPerPageOptions = {[25,50,75,100]}
paginationPerPage = {25}
subHeader
subHeaderComponent={[subHeaderComponent1,subHeaderComponent]}
onChangePage={() => setExpandableRowShow(false)}
expandableRowExpanded={() => expendableRowShow}
/>
)

}

export default RewardedUsers;