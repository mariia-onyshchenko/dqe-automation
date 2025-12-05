*** Settings ***
Library           SeleniumLibrary
Library           helper.py
Library           OperatingSystem

*** Variables ***
${REPORT_FILE}       C:/Users/mariia_onyshchenko/dqe_automation/dqe-automation/Selenium Introduction/report.html
${PARQUET_FOLDER}    C:/Users/mariia_onyshchenko/dqe_automation/dqe-automation/parquet_data
${FILTER_DATE}       2025-11-02

*** Test Cases ***
Validate Report Table
    Open Browser    file:///${REPORT_FILE}    chrome
    Maximize Browser Window
    Sleep    2s

    ${json}=    Execute Javascript
    ...    var gd = document.querySelector('.plotly-graph-div');
    ...    if (!gd || !gd.data) return {header: [], cells: []};
    ...    for (var i=0; i<gd.data.length; i++) {
    ...        if (gd.data[i].type === 'table') {
    ...            return {header: gd.data[i].header.values, cells: gd.data[i].cells.values};
    ...        }
    ...    }
    ...    return {header: [], cells: []};

    Should Not Be Empty    ${json["header"]}    msg=No Plotly table found in the report

    ${df_html}=    Plotly Table To Df    ${json["header"]}    ${json["cells"]}
    ${df_parquet}=    Load_Parquet_By_Date    ${PARQUET_FOLDER}    ${FILTER_DATE}
    Compare_And_Fail    ${df_html}    ${df_parquet}    ${FILTER_DATE}

    [Teardown]    Close Browser
