document.addEventListener('DOMContentLoaded', function() {
    const helpButton = document.querySelector('.help-button');
    const tipsButton = document.querySelector('.optimization-button');
    const updateDataButton = document.querySelector('#update-data-btn');

    const tipsTooltipText = "1) Provide detailed specifics, including the record type (e.g., PFA Assessment, Product Inquiry) and field names (e.g., FDA Reporting Decision, CIC).\n\n" +
    "2) Ensure accuracy in your queries; the model requires precise terms and cannot correct spacing errors or typos to locate specific values.\n\n" +
    "3) Use specific terminology for DATE queries (e.g., DATE OPENED, DATE CLOSED).\n\n" +
    "4) Structure queries logically with record type, field name, and specific value (e.g., PFA Assessment, FDA Reporting Decision, \"to be reported\")."



    const helpTooltipText = "QUERY Tab:\nInitiate a search using natural language prompts, which are automatically translated into SQL queries for database retrieval.\n\n" +
    "DATA Tab:\nRetrieve and analyze TrackWise data based on the generated SQL queries.";

    const updateDataTooltipText = "Update table information if TrackWise database table or column names are changed.\n\nWARNING: This process takes approximately 10 minutes to complete!";

    function updateTooltip(text) {
        const tooltip = document.querySelector('.tooltip');
        tooltip.textContent = text;
    }

    helpButton.addEventListener('mouseover', () => {
        updateTooltip(helpTooltipText);
    });

    tipsButton.addEventListener('mouseover', () => {
        updateTooltip(tipsTooltipText);
    });

    updateDataButton.addEventListener('mouseover', () => {
        updateTooltip(updateDataTooltipText);
    });

    document.getElementById('update-data-btn').addEventListener('click', function() {
        updateData();
    });
    
    document.getElementById('export-data-btn').addEventListener('click', function() {
        exportData();
    });

    function checkTableExistence() {
        var table = document.querySelector('table.data-table.display.responsive');
        if (table) {
            document.getElementById('export-data-btn').style.display = 'block';
        } else {
            document.getElementById('export-data-btn').style.display = 'none';
        }
    }

    checkTableExistence();


    var observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            if (mutation.type === 'childList') {
                checkTableExistence();
            }
        });
    });

    observer.observe(document.body, { childList: true, subtree: true });

});
let flashingInterval = null;


document.querySelectorAll('.tab-button').forEach(button => {
    button.addEventListener('click', () => {
        if (button.classList.contains('disabled')) {
            return;
        }
        document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active', 'hover-animation'));
        document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));

        button.classList.add('active');
        document.getElementById(button.getAttribute('data-tab') + '-container').classList.add('active');

        // if (button.getAttribute('data-tab') === 'data') {
        //     disableHoverAnimation();
        // } else {
        //     enableHoverAnimation();
        // }
    });
});

document.getElementById('send-btn').addEventListener('click', function() {
    hideChatBox();
    sendMessage();
});

document.getElementById('user-input').addEventListener('keydown', function(event) {
    if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        hideChatBox();
        sendMessage();
    }
});

function hideChatBox() {
    var chatBox = document.getElementById('text-logo');
    chatBox.classList.add('hidden');
}

function sendMessage() {
    var userInput = document.getElementById('user-input').value;
    if (userInput.trim() === '') return;

    var userMessage = document.createElement('div');
    userMessage.classList.add('message', 'user-message');
    userMessage.innerHTML = '<p>' + userInput + '</p>';
    document.getElementById('chat-box').appendChild(userMessage);

    var loadingDots = document.createElement('div');
    loadingDots.classList.add('loading-dots');
    for (var i = 0; i < 3; i++) {
        var dot = document.createElement('span');
        dot.classList.add('dot');
        loadingDots.appendChild(dot);
    }
    var loadingMessage = document.createElement('div');
    loadingMessage.classList.add('message', 'loading-message');
    loadingMessage.appendChild(loadingDots);
    document.getElementById('chat-box').appendChild(loadingMessage);

    document.getElementById('user-input').value = '';

    scrollToBottom();
    disableTabButtons();

    nlp2sql(userInput);
}


function nlp2sql(userInput) {
    var sqlQuery = userInput.trim();

    if (sqlQuery === '') {
        alert('Please enter a SQL query.');
        return;
    }

    fetch('/query', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            sql_query: sqlQuery
        })
    })
    .then(response => response.json())
    .then(data => {
        var loadingMessage = document.querySelector('.message.loading-message');
        if (loadingMessage) {
            loadingMessage.parentNode.removeChild(loadingMessage);
        }

        var aiMessage = document.createElement('div');
        aiMessage.classList.add('message', 'ai-message');
        var aiParagraph = document.createElement('p');
        aiMessage.appendChild(aiParagraph);
        document.getElementById('chat-box').appendChild(aiMessage);

        typeWriter(aiParagraph, 'Generated Query:\n' + data.result)
        .then(() => {
                var sqlOutputDiv = document.getElementById('sql-output');
                var children = sqlOutputDiv.childNodes;

                for (var i = children.length - 1; i >= 0; i--) {
                    var child = children[i];
                    if (child.id !== 'export-data-btn') {
                        sqlOutputDiv.removeChild(child);
                    }
                }

                var table = document.createElement('table');
                table.classList.add('data-table', 'display', 'responsive');

                var headerRow = document.createElement('thead');
                var headerRowContent = '<tr>';
                for (var key in data.data[0]) {
                    headerRowContent += '<th>' + key + '</th>';
                }
                headerRowContent += '</tr>';
                headerRow.innerHTML = headerRowContent;

                var tbody = document.createElement('tbody');
                data.data.forEach(rowData => {
                    var row = '<tr>';
                    for (var key in rowData) {
                        row += '<td>' + rowData[key] + '</td>';
                    }
                    row += '</tr>';
                    tbody.innerHTML += row;
                });

                table.appendChild(headerRow);
                table.appendChild(tbody);
                sqlOutputDiv.appendChild(table);

                $(document).ready(function() {
                    $('.data-table').DataTable({
                        responsive: true,
                        paging: true,
                        pageLength: 15,
                        scrollX: true,
                        lengthMenu: [5, 10, 15],
                    });
                });

                enableTabButtons();
                scrollToBottom();
                toggleHoverAnimation();

            })
        document.getElementById('sql-input').value = '';
    })
}


function scrollToBottom() {
    var chatBox = document.getElementById('chat-box');
    chatBox.scrollTop = chatBox.scrollHeight;
}

function disableTabButtons() {
    document.querySelectorAll('.tab-button').forEach(button => {
        button.classList.add('disabled');
    });
}

function enableTabButtons() {
    document.querySelectorAll('.tab-button').forEach(button => {
        button.classList.remove('disabled');
    });
}

function toggleHoverAnimation() {
    const dataTabButton = document.querySelector('.tab-buttons button[data-tab="data"]');
    if (dataTabButton) {
        dataTabButton.classList.toggle('hover-animation');
    }
}

function disableHoverAnimation() {
    const dataTabButton = document.querySelector('.tab-buttons button[data-tab="data"]');
    if (dataTabButton) {
        dataTabButton.classList.remove('hover-animation');
    }
}

function typeWriter(textElement, text, delay = 5) {
    return new Promise((resolve, reject) => {
        let charIndex = 0;
        const typing = () => {
            if (charIndex < text.length) {
                if (text.charAt(charIndex) === '\n') {
                    textElement.innerHTML += '<br>'; 
                } else {
                    textElement.innerHTML += text.charAt(charIndex);
                }
                charIndex++;
                setTimeout(typing, delay);
            } else {
                resolve();
            }
        };
        typing();
    });
}


function updateData() {
    fetch('/update_data', {
        method: 'POST'
    })
    .then(response => response.json())
    .then(data => {
        alert(data.message); 
    })
    .catch(error => {
        console.error('Error updating data:', error);
        alert('Error updating data. Please try again later.');
    });
}

function exportData() {
    fetch('/export_data', {
        method: 'POST'
    })
    .then(response => response.blob())
    .then(blob => {
        const url = window.URL.createObjectURL(new Blob([blob]));
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        a.download = 'extracted_data.csv';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
    })
    .catch(error => {
        console.error('Error exporting data:', error);
        alert('Error exporting data. Please try again later.');
    });
}