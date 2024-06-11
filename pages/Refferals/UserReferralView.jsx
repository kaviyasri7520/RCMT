import React, { useEffect, useState, useMemo } from 'react';
import './UserReferralView.css'; 
import { GoTriangleDown, GoTriangleUp } from 'react-icons/go';
import { apiBaseURL } from "../../core/utils";
import FilterComponent from "../../components/filter/FilterComponents";
import DataTable from 'react-data-table-component';

const UserReferralView = () => {
  const [filteredData, setFilteredData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [sortKey, setSortKey] = useState("createdDate");
  const [sortDirection, setSortDirection] = useState("asc");
  const [filterText, setFilterText] = useState("");
  const [resetPaginationToggle, setResetPaginationToggle] = useState(false);
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState(10);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch(`https://${apiBaseURL()}/v2/rcmtcontroller/fetchreferraluserscount`);
        const result = await response.json();
        setFilteredData(result.data);
      } catch (error) {
        console.error('Error fetching data:', error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, []);

  const handleSort = (column, sortDirection) => {
    setSortKey(column.selector);
    setSortDirection(sortDirection);
  };

  const handlePageChange = (page) => {
    setPage(page);
  };

  const handlePerPageChange = (newPerPage, page) => {
    setPerPage(newPerPage);
    setPage(page);
  };

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
        backgroundColor: ' #333222',
        minHeight: '52px',
        borderBottomWidth: '1px',
        borderBottomStyle: 'solid',
      },
      denseStyle: {
        minHeight: '32px',
      },
    },
  }

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

  const filteredItems = useMemo(() => {
    return filteredData.filter(item =>
      JSON.stringify(item).toLowerCase().includes(filterText.toLowerCase())
    );
  }, [filteredData, filterText]);

  if (isLoading) {
    return (
      <div style={{
        position: 'fixed',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
        fontSize: '20px',
        color: '#333',
      }}>
        Loading...
      </div>
    );
  }

  return (
    <div>
      <div className="pagination-container">
        <DataTable
          columns={[
            {
              name: 'Referred By',
              selector: (row) => row.ReferredBy,
              sortable: true,
            },
            {
              name: 'Referral User',
              selector: (row) => row.ReferralUser,
              sortable: true,
            },
            {
              name: 'Created Date',
              selector: (row) => row.createdDate,
              sortable: true,
            },
          ]}
          data={filteredItems}
          customStyles={customStyles}
          subHeader
          subHeaderComponent={subHeaderComponent}
          pagination
          paginationPerPage={perPage}
          paginationRowsPerPageOptions={[10, 25, 50, 100]}
          paginationComponentOptions={{
            rowsPerPageText: 'Records per page:',
            rangeSeparatorText: 'out of',
            paginationComponent: () => (
              <div className="datatable-pagination">
                <DataTable.Pagination
                  page={page}
                  onPageChange={handlePageChange}
                  onPerPageChange={handlePerPageChange}
                />
              </div>
            ),
          }}
          sortFunction={handleSort}
        />
      </div>
    </div>
  );
};

export default UserReferralView;
