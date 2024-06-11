import React, { useState, useEffect } from 'react';
import "./login.css";
import Log from "../../assets/images/96.png"
import axios from 'axios';
import { useNavigate } from 'react-router';
import BtnLoadingSpinner from '../../components/loadingSpinner/BtnLoadingSpinner'


 function Login()  {
  let Navigate = useNavigate()
  const [status,setStatus] = useState()
 const [username,setUsername]=useState("")
 const [password,setPassword]=useState("")
 const [loading,setLoading] = useState(false)
 const [selectedCountry,setSelectedCountry] = useState("us")

  useEffect(() => {
    localStorage.removeItem('userId')
  },[])

const func_username=(event) =>{
setUsername(event.target.value)
}
const func_password=(event) =>{
setPassword(event.target.value)
  }

const handle_countryChange = ({target}) =>{
  setSelectedCountry(target.value)
  localStorage.setItem('selectedCountry',target.value);
}

     const navigate = async(e) =>{
      setLoading(true)
      e.preventDefault()
      axios.get(`http://192.168.1.15:4455/posts/login?username=${username}&password=${password}`)
      .then((res)=>{
        const data = res.data;       
        data.displayName ? setStatus(!status) : setStatus(status) 
        if(data.error){   
            setLoading(false)       
            alert('Invalid Username or Password')
          }
        if(data.displayName){
          localStorage.setItem("selectedCountry",selectedCountry)
          localStorage.setItem("employeeName",data.displayName)
          localStorage.setItem('isVerified','TGLverified')
          localStorage.setItem('userId',btoa(username.toLowerCase()))
        }
        else{
          localStorage.setItem("employeeName","")
          localStorage.setItem('isVerified','notVerified')
          localStorage.setItem('userId','')
        }
      }).catch((error)=> {console.log(error);setLoading(false);alert('request failed')})
      //  setStatus(!status) 
     }
    const loginform = (
  <div className='login-container'>
  <form className="login-form">
  <fieldset className='login-fieldset' >
  <img className='img' src={Log} alt="" />
  <h1 style={{color:"#5d1bd9"}}>SyenApp</h1>
  <h4>Welcome To RCMT</h4>
  <input   type="text"  name="username"  value={username}  onChange={func_username} placeholder='Enter Your ID' autoComplete='off' />
  <input type="password"  name="password" value={password}  autoComplete='off'  onChange={func_password} placeholder='Enter Your Password' />
  <div className="country-select-container">
          
  <input className='radion-btn' type="radio" value="us" id="us" name="select_country" onChange={handle_countryChange} defaultChecked/>
  <label htmlFor="us">US</label>
  <input className='radion-btn' type="radio" value="in" id="in" name="select_country" onChange={handle_countryChange}/>
  <label htmlFor="in">IN</label>
  </div>
  <button onClick={navigate} type="submit" >{loading?<BtnLoadingSpinner />:'LOGIN'}</button>
  </fieldset>
  </form>
  </div>  
    )

    return (
    status ? Navigate('/rewards-track') : loginform  
    )
  }

  export default Login;