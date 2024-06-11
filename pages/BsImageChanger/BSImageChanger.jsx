import React, { useEffect, useState, useMemo } from 'react'
import './BSImageChanger.css'
import { BsPlusCircleFill, BsGlobe2, BsFillImageFill, } from 'react-icons/bs'
import BtnLoadingSpinner from "../../components/loadingSpinner/BtnLoadingSpinner";
import axios from 'axios'
import DataTable from "react-data-table-component";
import FilterComponent from "../../components/filter/FilterComponents";
import { AiTwotoneEdit } from 'react-icons/ai'
import PopUpModal from "../../components/popUpModals/PopUpModal";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import { apiBaseURL } from '../../core/utils';
import { BsFilesAlt } from 'react-icons/bs';

const { apiStoreHeader } = require('../../config/config.json')
 
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
      backgroundColor: " #333222",
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

const BSImageChanger = () => {

  const [filterText, setFilterText] = useState("");
  const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
  const [pending,setPending] = useState(true)
  const [bannerList,setBannerList] = useState([])
  const [bannerDetails,setBannerDetails] = useState({})
  const [showEdit,setShowEdit] = useState(false)
  const [showAdd,setShowAdd] = useState(false)
  const [trackSubmit,setTrackSubmit] = useState(false)
  const [expandableRowShow,setExpandableRowShow] = useState(false)
  const [submitAction,setSubmitAction] = useState(false)

  useEffect(() => {
    axios(`https://${apiBaseURL()}/v2/rcmtcontroller/fetchstreamers`,
    {
      method:'get',
      headers:apiStoreHeader
    })
    .then((res) => {
      const result = res.data.data.map(item => ({
        ...item,
        isActive: item.isActive === 'true' || item.isActive === true
      }));
      setBannerList(result);
      setPending(false);
    })
    .catch((err) => {
      console.log(err);alert('err in fetch streamers')
    })
  },[trackSubmit])

  const func_duplicateRow = (row) => {
    // Copy row data
    const copiedRowData = { ...row };
    // Automatically open the "Add New" modal with the copied row data
    setShowAdd(true);
    setBannerDetails(copiedRowData);
  };
  
 
  
  const columns = [
    {
      name: <div>Category</div>,
      selector: (row) => row.category,
      sortable: true,
      center: "true",
    },
    {
      name: <div>Merchant Id</div>,
      selector: (row) => row.merchantId,
      sortable: true,
      center: "true",
    },
    {
      name: <div>Title</div>,
      selector: (row) => row.title,
      center: "true",
    },
    {
      name: <div>Description</div>,
      selector: (row) => row.description,
      center: "true",
    },
    {
      name: <div>Seq No</div>,
      selector: (row) => row.seqNo,
      center: "true",
    },
    {
      name: <div>isActive</div>,
      selector: (row) => JSON.stringify(row.isActive),
      sortable: true,
      center: "true",
    },
    {
      name: <div>Country</div>,
      selector: (row) => row.country,
      center: "true",
    },
    {
      name: <div>Edit</div>,

      selector: (row) => (
        <button className='bs-edit-btn' onClick={() => func_openEdit(row)} ><AiTwotoneEdit className='bs-edit-icon' />
          Edit
        </button>
      ),
      center: "true",
    },
    {
      name: <div>Duplicate</div>,
      cell: (row) => (
        <BsFilesAlt onClick={() => func_duplicateRow(row)} style={{ fontSize: "20px", color: "green", cursor: "pointer" }} />
      ),
      center: "true",
    },
    
  ];

  const handleInputkeyword = ({ target }) => {
    const keys = target.name;
    let value = target.value;
  
    // Validate the "Keyword" field to restrict numbers
    if (keys === 'keyword') {
      // Check if the input contains any numbers
      if (/\d/.test(value)) {
        // If numbers are found, remove them from the input value
        value = value.replace(/\d/g, '');
      }
    }
  
    // Update the state with the modified value
    setBannerDetails(prev => ({ ...prev, [keys]: value }));
  };


  const handleInput = ({target}) => {
    let keys = target.name
    let value = target.value
    if(keys === 'merchantId' || keys === 'isVideo' || keys === 'seqNo' || keys === 'rewardPoints'){
      if(keys === 'merchantId'){
        console.log(/^\-/.test(bannerDetails))
          if(target.value.includes('-',0)){
            console.log(Array.from(target.value.matchAll('-')))
            if(Array.from(target.value.matchAll('-')).length > 1 ){
              console.log(1)
              target.value = bannerDetails.merchantId
              let numberValue = target.value
              setBannerDetails(prev => ({...prev,[keys]:numberValue}))
            }
            else{
              let numberValue = value.replace(/[a-zA-Z]/g, "").replace(/[^\w\s/-]|_/gi, "")
              setBannerDetails(prev => ({...prev,[keys]:numberValue}))
            }
          }
          else{
            let numberValue = value.replace(/[a-zA-Z]/g, "").replace(/[^\w\s/-]|_/gi, "")
            setBannerDetails(prev => ({...prev,[keys]:numberValue}))
          }  
          if(/^\d/.test(target.value)){
            let numberValue = value.replace(/[a-zA-Z]/g, "").replace(/[^\w\s]|_/gi, "")
            setBannerDetails(prev => ({...prev,[keys]:numberValue}))
          }
      }
      else{
        let numberValue = value.replace(/[a-zA-Z]/g, "").replace(/[^\w\s]|_/gi, "")
        setBannerDetails(prev => ({...prev,[keys]:numberValue}))
      }   
    }
    else{
      if(keys === 'country'){
        setBannerDetails(prev => ({...prev,[keys]:value.toLowerCase()}))
      }
      else{
        setBannerDetails(prev => ({...prev,[keys]:value}))
      }
    }  
  }
  const func_openAdd = () => {
    setSubmitAction(false)
    setShowAdd(true)
    setBannerDetails({
      category:'',
      merchantId:'',
      title:'',
      description:'',
      isActive:'',
      isVideo:'',
      rewardPoints:'',
      clickURL:'',
      imageURL:'',
      country:'',
      domain:'',
      seqNo:'',
      key:'',
      keyword: ''
    })
  }

  const func_openEdit = (row) => {
    setSubmitAction(false)
    setBannerDetails(row)
    setShowEdit(true)
  }
  const func_closePop_up = () => {
    setShowEdit(false)
    setShowAdd(false)
    setBannerDetails({
        category:'',
        merchantId:'',
        title:'',
        description:'',
        isActive:'',
        isVideo:'',
        rewardPoints:'',
        clickURL:'',
        imageURL:'',
        country:'',
        domain:'',
        seqNo:'',
        key:'',
        keyword: ''
      })
  }
  

  
  const func_bsAdd = () => {
    setSubmitAction(true)
    
    Object.keys(bannerDetails).forEach((key) => { 
      if(bannerDetails[key] === ''){delete bannerDetails[key] }    
      })
    Object.keys(bannerDetails).forEach((key) => { 
          if(key === 'merchantId'  || key === 'seqNo' || key === 'isVideo' || key === 'rewardPoints'){ bannerDetails[key] = JSON.parse(bannerDetails[key]) }
      })
    let params = bannerDetails
    axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/addstreamer`,params,{
      header:apiStoreHeader
    })
    .then((res) => {
      setSubmitAction(false)
      const result = res.data
      result.status === 1 && toast.success(`${params.merchantId} added successfully`, { autoClose: 1500 })
      result.status === -1 && toast.error("failed to update")
      setTrackSubmit(!trackSubmit)
      setBannerDetails({
        category:'',
        merchantId:'',
        title:'',
        description:'',
        isActive:'',
        isVideo:'',
        rewardPoints:'',
        clickURL:'',
        imageURL:'',
        country:'',
        domain:'',
        seqNo:'',
        key:'',
        keyword: ''
      })
    })
    .catch((err) => {console.log(err);toast.error('failed to add');console.log(params)})
  }

  const func_bsUpdate = () =>{
    setSubmitAction(true)
    Object.keys(bannerDetails).forEach(key => { 
      if(bannerDetails[key] === ''){ delete bannerDetails[key] }
      })
    Object.keys(bannerDetails).forEach(key => {
        if(key === 'merchantId' || key === 'isActive' || key === 'seqNo' || key === 'rewardPoints'){ bannerDetails[key] = JSON.parse(bannerDetails[key]) }
      })
    let params = bannerDetails
    axios.put(`https://${apiBaseURL()}/v2/rcmtcontroller/updatestreamer`,params,
    {
      headers:apiStoreHeader
    })
    .then((res) => {
      const result = res.data
      result.status === 1 && toast.success(`${params.merchantId} updated successfully`, { autoClose: 1500 })
      result.status === -1 && toast.error("failed to update")
      console.log(result)
      setTrackSubmit(!trackSubmit)
      setShowEdit(false)
      setSubmitAction(false)
      setBannerDetails({
        category:'',
        merchantId:'',
        title:'',
        description:'',
        isActive:'',
        isVideo:'',
        rewardPoints:'',
        clickURL:'',
        imageURL:'',
        country:'',
        domain:'',
        seqNo:'',
        key:'',
        keyword: ''
      })
    })
    .catch((err) => {console.log(err);toast.error('failed to update');console.log(params)})
  }

  const editForm = (
    <PopUpModal show={showEdit} handleClose={func_closePop_up}>
      <div className='bs-popup-container'>
        <div className='content-container'>
          <div className='bs-action' >Edit</div>
        <div className='bsimage-form' >
          <div className='bs-input-row' >
          <div className='bs-input-cell'><label>* Category : </label><input  name='category' className='seg1-input'  onChange={handleInput} value={bannerDetails.category} autoComplete='off' /></div>
          <div className='bs-input-cell'><label>* Merchant Id : </label><input name='merchantId' className='seg2-input'  onChange={handleInput} value={bannerDetails.merchantId} autoComplete='off' /></div>
          <div className='bs-input-cell'><label>* Description : </label><div><input name='description' className='seg3-input' onChange={handleInput} value={bannerDetails.description} autoComplete='off' />&nbsp;&nbsp;&nbsp;&nbsp;</div></div>
          </div>
          <div className='bs-input-row' >
          <div className='bs-input-cell'><label>Is Active : </label><select name='isActive' className='seg1-input'  onChange={handleInput} value={bannerDetails.isActive} autoComplete='off' >
            <option value={true}>true</option>
            <option value={false}>false</option>
          </select></div>
          <div className='bs-input-cell'><label>Seq No : </label><input name='seqNo' className='seg2-input'  onChange={handleInput} value={bannerDetails.seqNo} autoComplete='off' /></div>
          <div className='bs-input-cell'><label>* Click URL : </label><div><input name='clickURL' className='seg3-input'  onChange={handleInput} value={bannerDetails.clickURL} autoComplete='off' /><BsGlobe2 className='bs-icons' title='view website' onClick={() => window.open(bannerDetails.clickURL, '_blank')} /></div></div>
          </div>
          <div className='bs-input-row' >
          <div className='bs-input-cell'><label>Title : </label><input name='title' className='seg1-input' onChange={handleInput} value={bannerDetails.title} autoComplete='off' /></div>     
          <div className='bs-input-cell'><label>* Country : </label><input name='country' className='seg2-input'  onChange={handleInput} value={bannerDetails.country} autoComplete='off' /></div>
          <div className='bs-input-cell'><label>Domain : </label><div><input name='domain' className='seg3-input'  onChange={handleInput} value={bannerDetails.domain} autoComplete='off' />&nbsp;&nbsp;&nbsp;&nbsp;</div></div>
          </div>
          <div className='bs-input-row' >
          <div className='bs-input-cell'><label>Reward Points : </label><input name='rewardPoints' className='seg1-input' onChange={handleInput} value={bannerDetails.rewardPoints} autoComplete='off' /></div>   
          <div className='bs-input-cell'><label>Key : </label><input name='key' className='seg2-input'   onChange={handleInput} value={bannerDetails.key} autoComplete='off' /></div>  
          <div className='bs-input-cell'><label>* Image URL : </label><div><input name='imageURL' className='seg3-input'  onChange={handleInput} value={bannerDetails.imageURL} autoComplete='off' /><BsFillImageFill  className='bs-icons' title='view image' onClick={() => window.open(bannerDetails.imageURL, '_blank')} /></div></div>
          </div>
          <div className='bs-input-row' >
          <div className='bs-input-cell'><label>Is Video : </label><select  name='isVideo' className='seg1-input'  onChange={handleInput} value={bannerDetails.isVideo} autoComplete='off' >
            <option style={{display:'none'}} value=''></option>
            <option value={0}>0</option>
            <option value={1}>1</option> 
            </select></div>
            <div className='bs-input-cell'><label> Keyword : </label><div><input name='keyword' className='seg3-input' onChange={handleInputkeyword} value={bannerDetails.keyword} autoComplete='off' />&nbsp;&nbsp;&nbsp;&nbsp;</div></div>
         </div>
          <div className='bs-input-row' >
          <button className='bs-submit' onClick={func_bsUpdate}>{submitAction?<BtnLoadingSpinner />:'Submit'}</button>
          </div>
        </div>
      </div>
      <div className='bs-action' >Image Preview</div>
      <img src={bannerDetails.imageURL} alt='preview' />
      </div>
    </PopUpModal>
  )

  const addForm = (
    <PopUpModal show={showAdd} handleClose={func_closePop_up}>
      <div className='bs-popup-container'>
      <div className='content-container'>
        <div className='bs-action'>Add New</div>
        <div className='bsimage-form' >
          <div className='bs-input-row' >
          <div className='bs-input-cell'><label>* Category : </label><input  name='category' className='seg1-input'  onChange={handleInput} value={bannerDetails.category} autoComplete='off' /></div>
          <div className='bs-input-cell'><label>* Merchant Id : </label><input name='merchantId' className='seg2-input'  onChange={handleInput} value={bannerDetails.merchantId} autoComplete='off' /></div>
          <div className='bs-input-cell'><label>* Description : </label><div><input name='description' className='seg3-input' onChange={handleInput} value={bannerDetails.description} autoComplete='off' />&nbsp;&nbsp;&nbsp;&nbsp;</div></div>
          </div>
          <div className='bs-input-row' >
          <div className='bs-input-cell'><label>Is Active : </label><select name='isActive' className='seg1-input'  onChange={handleInput} value={bannerDetails.isActive} autoComplete='off' >
            <option style={{display:'none'}} value=''></option>
            <option value={true}>true</option>
            <option value={false}>false</option>
          </select></div>
          <div className='bs-input-cell'><label>Seq No : </label><input name='seqNo' className='seg2-input'  onChange={handleInput} value={bannerDetails.seqNo} autoComplete='off' /></div>
          <div className='bs-input-cell'><label>* Click URL : </label><div><input name='clickURL' className='seg3-input'  onChange={handleInput} value={bannerDetails.clickURL} autoComplete='off' /><BsGlobe2 className='bs-icons' title='view website' onClick={() => window.open(bannerDetails.clickURL, '_blank')} /></div></div>
          </div>
          <div className='bs-input-row' >
          <div className='bs-input-cell'><label>Title : </label><input name='title' className='seg1-input' onChange={handleInput} value={bannerDetails.title} autoComplete='off' /></div>     
          <div className='bs-input-cell'><label>* Country : </label><input name='country' className='seg2-input'  onChange={handleInput} value={bannerDetails.country} autoComplete='off' /></div>
          <div className='bs-input-cell'><label>Domain : </label><div><input name='domain' className='seg3-input'  onChange={handleInput} value={bannerDetails.domain} autoComplete='off' /><BsGlobe2 className='bs-icons' title='view website' onClick={() => window.open(bannerDetails.domain, '_blank')} /></div></div>
          </div>
          <div className='bs-input-row' >
          <div className='bs-input-cell'><label>Reward Points : </label><input name='rewardPoints' className='seg1-input' onChange={handleInput} value={bannerDetails.rewardPoints} autoComplete='off' /></div>   
          <div className='bs-input-cell'><label>Key : </label><input name='key' className='seg2-input'  onChange={handleInput} value={bannerDetails.key} autoComplete='off'  /></div>  
          <div className='bs-input-cell'><label>* Image URL : </label><div><input name='imageURL' className='seg3-input'  onChange={handleInput} value={bannerDetails.imageURL} autoComplete='off' /><BsFillImageFill  className='bs-icons' title='view image' onClick={() => window.open(bannerDetails.imageURL, '_blank')} /></div></div>

          </div>
          <div className='bs-input-row' >

          <div className='bs-input-cell'><label>Is Video : </label><select  name='isVideo' className='seg1-input'  onChange={handleInput} value={bannerDetails.isVideo} autoComplete='off' >
            <option style={{display:'none'}} value=''></option>
            <option value={0}>0</option>
            <option value={1}>1</option>
          </select>

          </div>
          <div className='bs-input-cell'><label> Keyword : </label><div><input name='keyword' className='seg3-input' onChange={handleInputkeyword} value={bannerDetails.keyword} autoComplete='off' />&nbsp;&nbsp;&nbsp;&nbsp;</div></div>

         </div>
          <div className='bs-input-row' >
          <button className='bs-submit' onClick={submitAction?null:func_bsAdd}>{submitAction?<BtnLoadingSpinner />:'Submit'}</button>
          </div>
        </div>
      </div>
      <div className='bs-action' >Image Preview</div>
      <img src={bannerDetails.imageURL} alt='preview ' />
      </div>
    </PopUpModal>
  )
    //filter
    const filteredItems = bannerList.filter(
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

    const subHeaderComponent2 = (  
        <button key="addnew-button" className='bs-add-btn' onClick={func_openAdd} ><BsPlusCircleFill className='bs-add-icon' />Add New</button>
    )
    
    const ExpandedComponent = ({ data }) => (
      <table className='bs-extended-table'>
        <tbody>
          <tr className='bs-extended-table-tr'>
            <th colSpan={2} className="thead">Additional Details</th>
          </tr>
          <tr className='bs-extended-table-tr'>
            <td >Description </td>
            <td >{data.description}</td>
          </tr>
          {/* <tr className='bs-extended-table-tr'>
            <td>Domain </td>
            <td><a href={data.domain} target='_blank' rel='noreferrer'>{data.domain}</a></td>
          </tr> */}
          <tr className='bs-extended-table-tr'>
          <td >Click URL </td>
          <td ><a href={data.clickURL} rel='noreferrer' target='_blank'>{data.clickURL}</a></td>
          </tr>
          <tr className='bs-extended-table-tr'>
            <td>Image URL </td>
            <td><a href={data.imageURL} target='_blank' rel='noreferrer'>{data.imageURL}</a></td>
          </tr>
        </tbody>
      </table>
    );

  return (
    <div>
       {/* {Header}
       {<ContentEditor />} */}
       
       <DataTable
        columns={columns}
        customStyles={customStyles}
        data={filteredItems}
        progressPending={pending}
        subHeader
        subHeaderComponent={[subHeaderComponent1,subHeaderComponent2]}
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
        expandableRowExpanded={() => expandableRowShow}
      />
      {addForm}
      {editForm}
      <ToastContainer toastClassName={"custom-toast"} />
    </div>
  )
}

export default BSImageChanger