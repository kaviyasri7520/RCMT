export const fmtcColumn = [
    {
        name: <div>Merchant Name</div>,
        selector: row =>row.merchantName,
        center:"true"
    },
    {
        name: <div>Affliate Platform</div>,
        selector: row =>row.affliatePlatform,
        center:"true"
    },
    {
        name: <div>Amount</div>,
        selector: row => row.amount,
        center:"true"
    },
    {
        name: <div>Transaction Date</div>,
        selector: row => row.transactionDate,
        sortable:true,
        center:"true"
    },
    // {
    //     name:<div>Actions</div>,
    //     selector:row =><div  style={{ fontSize: "15px ", fontFamily:"sans-serif",color:'black'}}  ><button style={{color:"white",borderRadius:"10px",border:"0px solid white",background:"#008000",width:"85px",height:"35px",borderStyle:"none",cursor:"pointer"}}>VIEW</button> {(row.button)}</div>,
    //     center:"true"
    // }
  ];
export const rakutenColumn = [
    {
        name: <div>Merchant Id</div>,
        selector: row =>row.merchantId,
        sortable:true,
        center:"true"
    },
    {
        name: <div>Merchant Name</div>,
        selector: row =>row.merchantName,
        center:"true"
    },
    {
        name: <div>Affliate Platform</div>,
        selector: row => row.affliatePlatform,
        center:"true"
    },
    {
        name: <div>Amount</div>,
        selector: row => row.amount,
        center:"true"
    },
    {
        name: <div>State</div>,
        selector: row => row.state,
        sortable:true,
        center:"true"
    },
    {
        name: <div>Transaction Date</div>,
        selector: row => row.TransactionDate,
        sortable:true,
        center:"true"
    },
    // {
    //     name:<div>Actions</div>,
    //     selector:row =><div  style={{ fontSize: "15px ", fontFamily:"sans-serif",color:'black'}}  ><button style={{color:"white",borderRadius:"10px",border:"0px solid white",background:"#008000",width:"85px",height:"35px",borderStyle:"none",cursor:"pointer"}}>VIEW</button> {(row.button)}</div>,
    //     center:"true"
    // }
  ];


export const ImpactColumn = [
    
    {
        name: <div>Merchant Name</div>,
        selector: row =>row.merchantName,
        center:"true"
    },
    {
        name: <div>User Name</div>,
        selector: row =>row.userName,
        sortable:true,
        center:"true"
    },
    {
        name: <div>Amount</div>,
        selector: row => row.amount,
        center:"true"
    },
    {
        name: <div>State</div>,
        selector: row => row.state,
        center:"true"
    },
    {
        name: <div>Transaction Date</div>,
        selector: row => row.transactionDate,
        sortable:true,
        center:"true"
    },
    // {
    //     name:<div>Actions</div>,
    //     selector:row =><div  style={{ fontSize: "15px ", fontFamily:"sans-serif",color:'black'}}  ><button style={{color:"white",borderRadius:"10px",border:"0px solid white",background:"#008000",width:"85px",height:"35px",borderStyle:"none",cursor:"pointer"}}>VIEW</button> {(row.button)}</div>,
    //     center:"true"
    // }
  ];


export const cjColumn = [
 
    {
        name: <div>Merchant Name</div>,
        selector: row =>row.merchantName,
        center:"true"
    },
    {
        name: <div>user Name</div>,
        selector: row =>row.userName,
        center:"true"
    },
    {
        name: <div>State</div>,
        selector: row => row.state,
        center:"true"
    },
    {
        name: <div>Amount</div>,
        selector: row => row.amount,
        center:"true"
    },
    {
        name: <div>Transaction Date</div>,
        selector: row => row.transactionDate,
        sortable:true,
        center:"true"
    },
    // {
    //     name: <div>publisher Commissions</div>,
    //     selector: row => row.publisherCommissions,
    //     center:"true"
    // },
    // {
    //     name:<div>Actions</div>,
    //     selector:row =><div  style={{ fontSize: "15px ", fontFamily:"sans-serif",color:'black'}}  ><button style={{color:"white",borderRadius:"10px",border:"0px solid white",background:"#008000",width:"85px",height:"35px",borderStyle:"none",cursor:"pointer"}}>VIEW</button> {(row.button)}</div>,
    //     center:"true"
    // }
  ];