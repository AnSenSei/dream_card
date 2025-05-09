console.log("draw.js loaded");

const API_BASE_URL = 'http://localhost:8080/gacha/api/v1'; // This should match your backend API base path

async function drawCards(packId, numToDraw = 1) {
    const displayArea = document.getElementById('cards-display-area');
    displayArea.innerHTML = '<p>Drawing cards...</p>'; // Loading indicator
    console.log(`Attempting to draw ${numToDraw} cards from pack: ${packId} via API: ${API_BASE_URL}/packs/${packId}/draw`);

    try {
        const response = await fetch(`${API_BASE_URL}/packs/${packId}/draw`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                // 'Authorization': 'Bearer YOUR_TOKEN_HERE'
            },
            body: JSON.stringify({ num_cards: numToDraw })
        });

        if (!response.ok) {
            let errorDetail = `Error drawing cards: ${response.statusText}`;
            try {
                const errorData = await response.json();
                errorDetail = errorData.detail || errorDetail;
            } catch (e) {
                // Could not parse JSON, stick with statusText
            }
            throw new Error(errorDetail);
        }

        const drawnCards = await response.json();
        displayDrawnCards(drawnCards);

    } catch (error) {
        console.error('Failed to draw cards:', error);
        displayArea.innerHTML = `<p style="color: red;">Error drawing cards: ${error.message}</p>`;
    }
}

function displayDrawnCards(cards) {
    const displayArea = document.getElementById('cards-display-area');
    if (!cards || cards.length === 0) {
        displayArea.innerHTML = '<p>No cards were drawn. Try again!</p>';
        return;
    }

    displayArea.innerHTML = ''; // Clear previous results or loading message

    cards.forEach(card => {
        const cardElement = document.createElement('div');
        cardElement.classList.add('card-item');
        // Use a placeholder if image_url is missing or empty
        const imageUrl = card.image_url ? card.image_url : 'https://via.placeholder.com/150/CCCCCC/000000?Text=No+Image';
        cardElement.innerHTML = `
            <img src="${imageUrl}" alt="${card.name}">
            <h4>${card.name}</h4>
            <p>Rarity: ${card.rarity}</p>
        `;
        cardElement.classList.add(`rarity-${card.rarity.toLowerCase()}`); 
        displayArea.appendChild(cardElement);
    });
}

// Add some basic styling for .card-item and .cards-container to style.css later
// e.g., in style.css:
/*
.cards-container {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    justify-content: center;
}
.card-item {
    border: 1px solid #ddd;
    border-radius: 8px;
    padding: 1rem;
    width: 150px; 
    text-align: center;
    background-color: #fff;
}
.card-item img {
    max-width: 100%;
    height: auto;
    border-bottom: 1px solid #eee;
    margin-bottom: 0.5rem;
}
.rarity-common { border-left: 5px solid grey; }
.rarity-rare { border-left: 5px solid skyblue; }
.rarity-epic { border-left: 5px solid purple; }
.rarity-legendary { border-left: 5px solid gold; }
*/ 