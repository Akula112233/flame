const BASE_URL = "http://44.199.216.233:8080/";
document.addEventListener("DOMContentLoaded", () => {
  const searchForm =
    document.getElementById("search-form") ||
    document.getElementById("results-search-form");
  const searchInput =
    document.getElementById("search-input") ||
    document.getElementById("results-search-input");
  const searchResults = document.getElementById("search-results");
  const loadingIndicator = document.getElementById("loading");
  const suggestionsContainer = document.getElementById("suggestions");

  let currentPage = 1;
  let loading = false;
  let debounceTimer;

  if (searchForm) {
    searchForm.addEventListener("submit", (e) => {
      e.preventDefault();
      const searchTerm = searchInput.value.trim();
      if (searchTerm) {
        window.location.href = `search-results.html?q=${encodeURIComponent(
          searchTerm
        )}`;
      }
    });
  }

  if (searchInput) {
    searchInput.addEventListener("input", () => {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        const query = searchInput.value.trim();
        if (query) {
          fetchSuggestions(query);
        } else {
          hideSuggestions();
        }
      }, 300);
    });

    searchInput.addEventListener("keydown", (e) => {
      if (e.key === "ArrowDown" || e.key === "ArrowUp") {
        e.preventDefault();
        navigateSuggestions(e.key === "ArrowDown" ? 1 : -1);
      } else if (e.key === "Enter") {
        const selectedSuggestion = suggestionsContainer.querySelector(
          ".suggestion-item:focus"
        );
        if (selectedSuggestion) {
          searchInput.value = selectedSuggestion.textContent;
          hideSuggestions();
        }
      }
    });
  }

  document.addEventListener("click", (e) => {
    if (
      !searchInput.contains(e.target) &&
      !suggestionsContainer.contains(e.target)
    ) {
      hideSuggestions();
    }
  });

  function fetchSuggestions(query) {
    const script = document.createElement("script");
    script.src = `https://suggestqueries.google.com/complete/search?client=chrome&q=${encodeURIComponent(
      query
    )}&callback=handleSuggestions`;
    document.body.appendChild(script);
    document.body.removeChild(script);
  }

  window.handleSuggestions = function (data) {
    const suggestions = data[1];
    if (suggestions.length > 0) {
      displaySuggestions(suggestions);
    } else {
      hideSuggestions();
    }
  };

  function displaySuggestions(suggestions) {
    suggestionsContainer.innerHTML = suggestions
      .map(
        (suggestion) =>
          `<div class="suggestion-item" tabindex="0">${suggestion}</div>`
      )
      .join("");
    suggestionsContainer.classList.add("show");
    searchInput.parentElement.classList.add("has-suggestions");

    const suggestionItems =
      suggestionsContainer.querySelectorAll(".suggestion-item");
    suggestionItems.forEach((item) => {
      item.addEventListener("click", () => {
        searchInput.value = item.textContent;
        hideSuggestions();
        searchForm.dispatchEvent(new Event("submit"));
      });
    });
  }

  function hideSuggestions() {
    suggestionsContainer.classList.remove("show");
    searchInput.parentElement.classList.remove("has-suggestions");
  }

  function navigateSuggestions(direction) {
    const suggestions =
      suggestionsContainer.querySelectorAll(".suggestion-item");
    const focusedElement = document.activeElement;
    let nextIndex = -1;

    if (focusedElement.classList.contains("suggestion-item")) {
      const currentIndex = Array.from(suggestions).indexOf(focusedElement);
      nextIndex =
        (currentIndex + direction + suggestions.length) % suggestions.length;
    } else {
      nextIndex = direction > 0 ? 0 : suggestions.length - 1;
    }

    suggestions[nextIndex].focus();
  }

  const luckyButton = document.querySelector(".search-button:nth-child(2)");
  if (luckyButton) {
    luckyButton.addEventListener("click", () => {
      window.location.href = "https://www.google.com/doodles";
    });
  }

  function getParameterByName(name, url = window.location.href) {
    name = name.replace(/[\[\]]/g, "\\$&");
    const regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
      results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return "";
    return decodeURIComponent(results[2].replace(/\+/g, " "));
  }

  function generateDummyResults(query, page) {
    const results = [];
    const startIndex = (page - 1) * 10;
    for (let i = 1; i <= 10; i++) {
      results.push({
        title: `${query} - Result ${startIndex + i}`,
        url: `https://example.com/result${startIndex + i}`,
        preview: `This is a snippet for result ${
          startIndex + i
        } related to ${query}. It contains some information about the search topic.`,
      });
    }
    return results;
  }

  async function getDummyResults(query, maxResults = 10) {
    results = [];
    for (let i = 1; i <= maxResults; i++) {
      results.push({
        title: `${query} - Result ${i}`,
        url: `https://example.com/result${i}`,
        preview: `This is a snippet for result ${i} related to ${query}. It contains some information about the search topic.`,
      });
    }
    return results;
  }

  async function getSearchResults(query, maxResults = 10) {
    const url = `${BASE_URL}search?q=${encodeURIComponent(
      query
    )}&n=${maxResults}`;
    console.log(url);
    try {
      const response = await fetch(url, {
        method: "GET",
      });
      if (!response.ok) {
        throw new Error("Network response was not ok");
      }
      const data_1 = await response.json();
      console.log(data_1);
      return data_1;
    } catch (error) {
      console.error("Error fetching search results:", error);
      return [];
    }
  }

  function displaySearchResults(query, page, append = false) {
    if (searchResults) {
      // const dummyResults = generateDummyResults(query, page);
      getSearchResults(query, 10 * page).then((additionalResults) => {
        console.log(additionalResults);
        lastTen = additionalResults.slice(10 * (page - 1), 10 * page);
        console.log(lastTen);
        const resultsHTML = additionalResults
          .map(
            (result) => `
                <div class="search-result">
                    <h3><a href="${result.url}">${result.url}</a></h3>
                    <div class="url">${result.url}</div>
                    <div class="snippet">${result.preview}</div>
                </div>
            `
          )
          .join("");

        output = resultsHTML == searchResults.innerHTML;

        if (append) {
          searchResults.insertAdjacentHTML("beforeend", resultsHTML);
        } else {
          searchResults.innerHTML = resultsHTML;
        }

        if (searchInput) {
          searchInput.value = query;
        }
        return output;
      });
    }
  }
  var updated = true;
  function loadMoreResults() {
    if (loading) {
      return;
    }
    if (!updated) return;
    loading = true;
    loadingIndicator.classList.add("visible");

    setTimeout(() => {
      currentPage++;
      updated = displaySearchResults(searchQuery, currentPage, false);
      loading = false;
      loadingIndicator.classList.remove("visible");
    }, 1000);
  }

  function isBottomOfPage() {
    return (
      window.innerHeight + window.scrollY >= document.body.offsetHeight - 100
    );
  }

  function handleScroll() {
    if (isBottomOfPage() && !loading) {
      loadMoreResults();
    }
  }

  const searchQuery = getParameterByName("q");
  if (searchQuery && searchResults) {
    displaySearchResults(searchQuery, currentPage);
    window.addEventListener("scroll", handleScroll);
  }
});
