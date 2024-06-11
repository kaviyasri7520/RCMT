// src/PopularSearches.js
import React, { useEffect, useState } from 'react';
import './PopularSearch.css';
import { apiBaseURL } from "../../core/utils";

const PopularSearch = () => {
    const [searchTerms, setSearchTerms] = useState([]);
    const [inputValue, setInputValue] = useState('');
    const [popupVisible, setPopupVisible] = useState(false);
    const [selectedTerm, setSelectedTerm] = useState('');

    useEffect(() => {
        fetch(`https://${apiBaseURL()}/textsearch/getpopularsearch`, {
            headers: {
                'appapielastic': 'appapielastic'
            }
        })
            .then(response => response.json())
            .then(data => {
                setSearchTerms(data.data);
            })
            .catch(error => {
                console.error('Error fetching popular searches:', error);
            });
    }, []);

    const handleSubmit = (e) => {
        e.preventDefault();
        if (inputValue.trim() === '') {
            alert('Please enter a keyword.');
            return;
        }
        fetch(`https://${apiBaseURL()}/textsearch/addpopulorsearch`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'appapielastic': 'appapielastic'
            },
            body: JSON.stringify({ textitem: inputValue })
        })
            .then(response => response.json())
            .then(data => {
                console.log('Success:', data);
                alert("Added Successfully")
                setSearchTerms([...searchTerms, inputValue]);
                setInputValue("")
            })
            .catch(error => {
                console.error('Error adding popular search:', error);
            });
    };

    const handleTermClick = (term) => {
        setSelectedTerm(term);
        setPopupVisible(true);
    };
  

    const handleRemove = () => {
        fetch(`https://${apiBaseURL()}/textsearch/removepopulorsearch`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'appapielastic': 'appapielastic'
            },
            body: JSON.stringify({ textitem: selectedTerm })
        })
            .then(response => response.json())
            .then(data => {
                console.log('Success:', data);
                alert("Removed Successfully")
                setSearchTerms(searchTerms.filter(term => term !== selectedTerm));
                setPopupVisible(false);
            })
            .catch(error => {
                console.error('Error removing popular search:', error);
            });
    };

    const handleClosePopup = () => {
        setPopupVisible(false);
    };

    return (
        <div>
            <form className="form-item" onSubmit={handleSubmit}>
                <input 
                    type="text" 
                    id="searchkeyword" 
                    autoComplete="off" 
                    value={inputValue} 
                    onChange={(e) => setInputValue(e.target.value)} 
                />
                <label id='headers' htmlFor="searchkeyword">Enter Popular Search Keyword</label>
                <button className="button" type="submit"><span>Submit</span></button>
            </form>

            <div id="showlist">
                <ul>
                    {searchTerms.map((term, index) => (
                        <li id="listitems" key={index} onClick={() => handleTermClick(term)}>
                            {term}
                        </li>
                    ))}
                </ul>
            </div>

            {popupVisible && (
                <div className="popup-overlay">
                    <div className="popup">
                        <p>Do you want to remove the keyword "{selectedTerm}"?</p>
                        <button className="button" onClick={handleRemove}><span>Yes</span></button>
                        <button className="button" onClick={handleClosePopup}><span>No</span></button>
                    </div>
                </div>
            )}
        </div>
    );
};

export default PopularSearch;
