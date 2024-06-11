import React from 'react'
import './PageNotFound.css'
import { AiFillWarning } from 'react-icons/ai'

const PageNotFound = () => {
  return (  
      <div className='page-404-container'>
        <div className='message-box'>
        <AiFillWarning className='not-found-alert' />
        <div className='not-found-code'>404 error</div>
        <div className='not-found-message'>Page Not Found</div>
        </div>
      </div>
  )
}

export default PageNotFound