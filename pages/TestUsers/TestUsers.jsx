import React, { useState,useMemo, useEffect } from 'react'
import './TestUsers.css'
import DataTable from 'react-data-table-component'
import FilterComponent from "../../components/filter/FilterComponents";
import axios from 'axios';
import { BsPlusCircleFill } from 'react-icons/bs'
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import { ImBin2 } from "react-icons/im";

const { testUsersURL,syenappIdTailIndia,syenappIdTailUsa } = require('../../config/config.json')

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

const TestUsers = () =>{

    const [filterText, setFilterText] = useState("");
    const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
    const [pending,setPending] = useState(true)
    const [syenappId,setSyenappId] = useState('')
    const [testUsersList,setTestUsersList] = useState([])
    const [track,setTrack] = useState(false)
    const [tail,setTail] = useState(syenappIdTailUsa)
    const alphabeticPattern = /[a-zA-Z]/

    useEffect(() => {
        axios.get(`${testUsersURL}/testusers-list`)
        .then((res) => {
            let data = res.data
            setTestUsersList(data)
            setPending(false)      
        })
        .catch((err) => {console.log(err);toast.error('failed to fetch test users');})
        let currentCountry = localStorage.getItem('selectedCountry')
        currentCountry.toLowerCase() === 'in' ? setTail(syenappIdTailIndia) : setTail(syenappIdTailUsa)
    },[track])

    const func_deleteId = (row) => {
      axios.put(`${testUsersURL}/delete-testuser`,row)
      .then((res) => {
        const data = res.data
        data.status === 1 && toast.success(`${row.syenappId} deleted successfully`, { autoClose: 1500 })
        setTrack(!track)
      })
      .catch((err) => {console.log(err);toast.error('failed to delete the user');})
    }

    let column = [
        {
         name: <div>SyenApp Id</div>,
         selector: (row) => row.syenappId,
        },
        {
          name: <div>Action</div>,
          button: true,
          cell: (row) => (  
                <ImBin2
                  style={{ fontSize: "17px", color: "red" ,cursor:"pointer"}}
                  title='delete the user'
                  onClick={() => func_deleteId(row)}
                />
          )
        }
    ]

    const handleInput = ({target}) => {
      setSyenappId(target.value.replace(/[^\w]|_/gi,'').toLowerCase()) 
    }

    const addTestUser = (e) => {
      e.preventDefault()
        if(syenappId !== ''&& alphabeticPattern.test(syenappId)){
          const params = {'syenappId':`${syenappId}${tail}`}
       
            axios.put(`${testUsersURL}/add-testuser`,params)
            .then((res) => {
                let result = res.data
                setTrack(!track)    
                result.status === 1 && toast.success(`${params.syenappId} added successfully`, { autoClose: 1500 })
                result.status === -1 && toast.error(`failed to add ${params.syenappId}`)
                result.status === 409 && toast.error(`${params.syenappId} is already exists`)
                setSyenappId('')
            })
            .catch((err) => {console.log(err);toast.error(`failed to add ${syenappId}`)})
          }         
        else{
            syenappId === "" && toast.error(`Enter the SyenApp Test Id`)
            if(syenappId !== "" && !alphabeticPattern.test(syenappId)){
              toast.error(`SyenApp Id must contain alphabets`)
            }  
        }
    }
        //filter
    const filteredItems = testUsersList.filter(
            (item) =>
              JSON.stringify(item).toLowerCase().indexOf(filterText.toLowerCase()) !==
              -1
          );
        
          //filter component inpput
    const subHeaderComponent1 = useMemo(() => {
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
      
    return(
        <div>
            <div className='user-id-input-container'>
              <form className='form'>
          <label className='user-id-label'>Testing Id :</label>
            <input className='user-id-input' name='syenappId' onChange={handleInput} value={syenappId} autoComplete='off'/>
            <input className='user-id-tail' name='syenappId' value={tail} disabled/>
            <button className='user-id-add-btn' onClick={addTestUser}><BsPlusCircleFill className='bs-add-icon' />Add</button>
              </form>
            </div>
        <DataTable
        columns={column}
        customStyles={customStyles}
        data={filteredItems}
        progressPending={pending}
        subHeader
        subHeaderComponent={[subHeaderComponent1]}
        pagination
        paginationRowsPerPageOptions={[25, 50, 75, 100]}
        paginationPerPage={25}
        paginationComponentOptions={{
          rowsPerPageText: "Records per page:",
          rangeSeparatorText: "out of",
        }}
      />
      <ToastContainer toastClassName={"custom-toast"} />
        </div>
    )
}
export default TestUsers