// Import necessary dependencies
import React, { useEffect, useState } from 'react';
import './Campaign.css'; 
import { apiBaseURL } from "../../core/utils";

const CampaignView = () => {
  // const [data, setData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch(`https://${apiBaseURL()}/v2/rcmtcontroller/fetchcampaignreferraluserscount`);
        const result = await response.json();
        // setData(result.data);
        setFilteredData(result.data); // Initialize filteredData with all data
      } catch (error) {
        console.error('Error fetching data:', error);
      } finally {
        setIsLoading(false); // Set loading to false regardless of success or failure
      }
    };

    fetchData();
  }, []); 


return (
    <div>

      
      <div className="campaign-container">
     
        {isLoading ? (
          <p>Loading...</p>
        ) : (
          filteredData.length === 0 ? (
            <p className="no-records-message">No records found</p>
          ) : (
            <table className="table">
              <thead className="th">
                <tr>
                  <th>Referral User</th>
                  <th>Referred By</th>
                 
                </tr>
              </thead>
              <tbody>
                {filteredData.map((item, index) => (
                  <tr key={index}>
                    <td className="td">{item.name}</td>
                    <td className="td">{item.userCount}</td>
                  
                  </tr>
                ))}
              </tbody>
            </table>
          )
        )}
           </div>
      </div>

  );
};

export default CampaignView;
