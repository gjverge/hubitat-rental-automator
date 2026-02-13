/* Copyright 2023 Dan Healy (thedanhealy.com)

Permission is hereby granted, free of charge, to any person obtaining a copy of this 
software and associated documentation files (the "Software"), to deal in the Software 
without restriction, including without limitation the rights to use, copy, modify, 
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to 
permit persons to whom the Software is furnished to do so, subject to the following 
conditions:

The above copyright notice and this permission notice shall be included in all copies 
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A 
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF 
CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE 
OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

/**
 * ============================================================================
 * RENTAL AUTOMATOR - Hubitat Application
 * ============================================================================
 *
 * A smart home automation app for managing AirBNB/OwnerRez rental properties.
 * Automatically programs door lock codes and switches Hubitat modes based on
 * guest check-in/check-out times from iCal calendar feeds.
 *
 * SUPPORTED CALENDAR FORMATS:
 * ---------------------------
 * 1. AirBNB iCal Format:
 *    - SUMMARY: "Reserved" for confirmed bookings
 *    - DTSTART/DTEND: Date only format (yyyyMMdd)
 *    - Door code: Extracted from "Last 4 Digits." field in description
 *    - Guest name: Not available (set to space)
 *
 * 2. OwnerRez iCal Format:
 *    - STATUS: "CONFIRMED" for confirmed bookings
 *    - DTSTART/DTEND: DateTime format (yyyyMMdd'T'HHmmss) with exact times
 *    - Door code: Extracted from "DoorCode" custom field
 *    - Guest name: Extracted from "FirstName" custom field
 *
 * STATE VARIABLES:
 * ----------------
 * - atomicState.automationEnabled: Boolean indicating if automation is currently active (renamed from 'enabled' which is a reserved Hubitat state key)
 * - atomicState.testCalendar: Boolean flag for calendar URL test mode
 * - atomicState.testCalendarUrlState: Boolean result of last calendar URL test
 * - state.iCalDictPrevious: Cached copy of last successful iCal data (fallback)
 * - state.ownerRezCheckinDate: Date object for OwnerRez check-in (when using exact times)
 * - state.ownerRezCheckoutDate: Date object for OwnerRez check-out (when using exact times)
 * - state.lastCalendarFetch: Timestamp of last calendar fetch (for rate limiting)
 *
 * ANALYTICS STATE VARIABLES:
 * --------------------------
 * - state.analytics.checkinSuccess: Count of successful check-ins
 * - state.analytics.checkinFailed: Count of failed check-ins
 * - state.analytics.checkoutSuccess: Count of successful check-outs
 * - state.analytics.checkoutFailed: Count of failed check-outs
 * - state.analytics.lockProgramSuccess: Count of successful lock programmings
 * - state.analytics.lockProgramFailed: Count of failed lock programmings
 * - state.analytics.lockDeleteSuccess: Count of successful lock code deletions
 * - state.analytics.lockDeleteFailed: Count of failed lock code deletions
 * - state.analytics.lastCheckin: Timestamp of last successful check-in
 * - state.analytics.lastCheckout: Timestamp of last successful check-out
 * - state.analytics.recentEvents: List of recent automation events (last 20)
 *
 * ============================================================================
 */

definition(
    name: "Rental Automator",
    namespace: "thedanhealy-rental-automator",
    author: "TheDanHealy",
    description: "Rental Automation for AirBNB and OwnerRez",
    category: "Convenience",
    iconUrl: "",
    iconX2Url: ""
)

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.regex.Pattern
import java.util.regex.Matcher
import groovy.json.JsonSlurper

// Minimum interval between calendar fetches in milliseconds (5 minutes)
@groovy.transform.Field static final Long CALENDAR_FETCH_MIN_INTERVAL = 300000

// Current application version for state migration
@groovy.transform.Field static final Integer APP_VERSION = 2

//--------------------------------------------------------------
// Calendar Parsing Functions (Hubitat-compatible, no classes)
//--------------------------------------------------------------

/**
 * Parses AirBNB iCal data into a list of event maps.
 * - SUMMARY: "Reserved" for confirmed bookings
 * - DTSTART/DTEND: Date-only format (yyyyMMdd)
 * - "Last 4 Digits.": Door code from description
 * @param iCalData Raw iCal string data
 * @return List of event maps with keys: summary, startDate, endDate, door, name
 */
private List<Map> parseAirBNBCalendar(String iCalData) {
    def events = []
    def eventMatches = (iCalData =~ /BEGIN:VEVENT[\s\S]*?END:VEVENT/).collect()

    debugLog "AirBNB parsing: Found ${eventMatches.size()} VEVENT blocks"

    eventMatches.each { match ->
        def event = [
            summary: extractProperty(match, "SUMMARY"),
            startDate: extractProperty(match, "DTSTART"),
            endDate: extractProperty(match, "DTEND"),
            door: extractProperty(match, "Last 4 Digits."),
            name: " "  // AirBNB doesn't provide guest name in iCal
        ]
        events << event
    }
    return events
}

/**
 * Parses OwnerRez iCal data into a list of event maps.
 * - STATUS: "CONFIRMED" for confirmed bookings
 * - DTSTART/DTEND: DateTime format (yyyyMMdd'T'HHmmss)
 * - DoorCode: Custom field with door code
 * - FirstName: Custom field with guest first name
 * @param iCalData Raw iCal string data
 * @return List of event maps with keys: summary, startDate, endDate, door, name
 */
private List<Map> parseOwnerRezCalendar(String iCalData) {
    def events = []
    def eventMatches = (iCalData =~ /BEGIN:VEVENT[\s\S]*?END:VEVENT/).collect()

    debugLog "OwnerRez parsing: Found ${eventMatches.size()} VEVENT blocks"

    eventMatches.each { match ->
        def event = [
            summary: extractProperty(match, "STATUS"),
            startDate: extractProperty(match, "DTSTART"),
            endDate: extractProperty(match, "DTEND"),
            door: extractProperty(match, "DoorCode"),
            name: extractProperty(match, "FirstName")
        ]
        events << event
    }
    return events
}

/**
 * Parses iCal data based on the configured calendar format.
 * @param iCalData Raw iCal string data
 * @param format The calendar format ("AirBNB" or "OwnerRez")
 * @return List of event maps
 */
private List<Map> parseCalendarData(String iCalData, String format) {
    switch(format) {
        case "AirBNB":
            return parseAirBNBCalendar(iCalData)
        case "OwnerRez":
            return parseOwnerRezCalendar(iCalData)
        default:
            log.error "Calendar parsing: Unknown format: ${format}"
            return []
    }
}

//--------------------------------------------------------------
// Common Helper Functions
//--------------------------------------------------------------

/**
 * Logs a debug message if debug mode is enabled.
 * Reduces boilerplate throughout the codebase.
 * @param message The message to log
 */
private void debugLog(String message) {
    if(debugMode) log.debug message
}

/**
 * Returns the expected event summary/status value based on calendar format.
 * AirBNB uses "Reserved" in SUMMARY field, OwnerRez uses "CONFIRMED" in STATUS field.
 * @return Expected summary string for confirmed bookings
 */
private String getExpectedEventSummary() {
    return calFormat == "AirBNB" ? "Reserved" : "CONFIRMED"
}

/**
 * Extracts the date portion (first 8 characters) from an iCal date string.
 * Handles both date-only (yyyyMMdd) and datetime (yyyyMMdd'T'HHmmss) formats.
 * @param dateStr The iCal date/datetime string
 * @return 8-character date string (yyyyMMdd) or original string if too short
 */
private String extractDateFromICalDate(String dateStr) {
    return dateStr?.length() >= 8 ? dateStr.substring(0, 8) : dateStr
}

//--------------------------------------------------------------
// Security Helper Functions
//--------------------------------------------------------------

/**
 * Validates that a calendar URL uses HTTPS protocol.
 * @param url The URL string to validate
 * @return true if URL is valid HTTPS, false otherwise
 */
private Boolean isValidSecureUrl(String url) {
    if (!url) return false
    return url.toLowerCase().startsWith("https://")
}

/**
 * Sanitizes guest names to prevent injection attacks and ensure safe usage in lock code names.
 * Removes special characters, limits length, and ensures alphanumeric with spaces only.
 * @param name The raw guest name from calendar data
 * @return Sanitized guest name safe for lock code naming
 */
private String sanitizeGuestName(String name) {
    if (!name || name.trim().isEmpty()) {
        return "Guest"
    }
    // Remove any characters that aren't alphanumeric or spaces
    String sanitized = name.replaceAll(/[^a-zA-Z0-9\s]/, "").trim()
    // Limit length to 20 characters to prevent issues with lock displays
    if (sanitized.length() > 20) {
        sanitized = sanitized.substring(0, 20).trim()
    }
    // Return "Guest" if sanitization removed everything
    return sanitized.isEmpty() ? "Guest" : sanitized
}

/**
 * Validates that a door code is a valid PIN.
 * Must be 4-8 digits, numeric only.
 * @param code The door code to validate
 * @return true if valid, false otherwise
 */
private Boolean isValidDoorCode(String code) {
    if(!code || code.trim().isEmpty()) return false
    if(code.length() < 4 || code.length() > 8) return false
    return code.matches("^[0-9]+\$")
}

/**
 * Masks a door code for safe logging (shows only last 2 digits).
 * @param code The door code to mask
 * @return Masked code string (e.g., "****56")
 */
private String maskCode(String code) {
    if (!code || code.length() < 2) return "****"
    return "*".multiply(code.length() - 2) + code.substring(code.length() - 2)
}

/**
 * Checks if enough time has passed since the last calendar fetch (rate limiting).
 * @return true if fetch is allowed, false if rate limited
 */
private Boolean canFetchCalendar() {
    def lastFetch = state.lastCalendarFetch
    if (!lastFetch) return true
    def now = new Date().getTime()
    return (now - lastFetch) >= CALENDAR_FETCH_MIN_INTERVAL
}

/**
 * Records the current time as the last calendar fetch time.
 */
private void recordCalendarFetch() {
    state.lastCalendarFetch = new Date().getTime()
}

//--------------------------------------------------------------
// Analytics Functions
//--------------------------------------------------------------

/**
 * Initializes the analytics state structure if not already present.
 * Called on app install and when analytics are reset.
 */
private void initializeAnalytics() {
    if (!state.analytics) {
        state.analytics = [
            checkinSuccess: 0,
            checkinFailed: 0,
            checkoutSuccess: 0,
            checkoutFailed: 0,
            lockProgramSuccess: 0,
            lockProgramFailed: 0,
            lockDeleteSuccess: 0,
            lockDeleteFailed: 0,
            lockProgramRetryTotal: 0,
            lockProgramFirstTry: 0,
            lockDeleteRetryTotal: 0,
            lockDeleteFirstTry: 0,
            lastCheckin: null,
            lastCheckout: null,
            recentEvents: []
        ]
    }
}

/**
 * Records an analytics event with timestamp.
 * @param eventType The type of event (e.g., "checkin_success", "checkout_failed")
 * @param details Optional details about the event
 */
private void recordAnalyticsEvent(String eventType, String details = "") {
    initializeAnalytics()
    def timestamp = new Date()
    def formattedTime = timestamp.format("yyyy-MM-dd HH:mm:ss")

    // Update counters based on event type
    switch(eventType) {
        case "checkin_success":
            state.analytics.checkinSuccess++
            state.analytics.lastCheckin = formattedTime
            break
        case "checkin_failed":
            state.analytics.checkinFailed++
            break
        case "checkout_success":
            state.analytics.checkoutSuccess++
            state.analytics.lastCheckout = formattedTime
            break
        case "checkout_failed":
            state.analytics.checkoutFailed++
            break
        case "lock_program_success":
            state.analytics.lockProgramSuccess++
            break
        case "lock_program_failed":
            state.analytics.lockProgramFailed++
            break
        case "lock_delete_success":
            state.analytics.lockDeleteSuccess++
            break
        case "lock_delete_failed":
            state.analytics.lockDeleteFailed++
            break
    }

    // Add to recent events list (keep last 20)
    def eventRecord = [
        timestamp: formattedTime,
        type: eventType,
        details: details
    ]

    if (!state.analytics.recentEvents) {
        state.analytics.recentEvents = []
    }
    state.analytics.recentEvents.add(0, eventRecord)  // Add to beginning
    if (state.analytics.recentEvents.size() > 20) {
        state.analytics.recentEvents = state.analytics.recentEvents.take(20)
    }
}

/**
 * Calculates the success rate for a given operation type.
 * @param successCount Number of successful operations
 * @param failedCount Number of failed operations
 * @return Success rate as percentage string (e.g., "95.5%")
 */
private String calculateSuccessRate(int successCount, int failedCount) {
    def total = successCount + failedCount
    if (total == 0) return "N/A"
    def rate = (successCount / total) * 100
    return String.format("%.1f%%", rate)
}

/**
 * Generates an HTML-formatted analytics summary for display in the UI.
 * @return HTML string with analytics data
 */
private String getAnalyticsSummary() {
    try {
        initializeAnalytics()
        def a = state.analytics

        def checkinRate = calculateSuccessRate(a.checkinSuccess ?: 0, a.checkinFailed ?: 0)
        def checkoutRate = calculateSuccessRate(a.checkoutSuccess ?: 0, a.checkoutFailed ?: 0)
        def lockProgramRate = calculateSuccessRate(a.lockProgramSuccess ?: 0, a.lockProgramFailed ?: 0)
        def lockDeleteRate = calculateSuccessRate(a.lockDeleteSuccess ?: 0, a.lockDeleteFailed ?: 0)

        def programAvg = (a.lockProgramSuccess ?: 0) > 0 ?
            ((a.lockProgramRetryTotal ?: 0) / a.lockProgramSuccess).setScale(1, java.math.RoundingMode.HALF_UP) : 0
        def deleteAvg = (a.lockDeleteSuccess ?: 0) > 0 ?
            ((a.lockDeleteRetryTotal ?: 0) / a.lockDeleteSuccess).setScale(1, java.math.RoundingMode.HALF_UP) : 0
        def programFirstTryPct = (a.lockProgramSuccess ?: 0) > 0 ?
            (((a.lockProgramFirstTry ?: 0) / a.lockProgramSuccess) * 100).setScale(1, java.math.RoundingMode.HALF_DOWN) : 0
        def deleteFirstTryPct = (a.lockDeleteSuccess ?: 0) > 0 ?
            (((a.lockDeleteFirstTry ?: 0) / a.lockDeleteSuccess) * 100).setScale(1, java.math.RoundingMode.HALF_DOWN) : 0

        def lastCheckinStr = "Never"
        def lastCheckoutStr = "Never"
        try {
            if(a.lastCheckin) {
                lastCheckinStr = (a.lastCheckin instanceof Date) ? a.lastCheckin.format("yyyy-MM-dd HH:mm") : a.lastCheckin.toString()
            }
            if(a.lastCheckout) {
                lastCheckoutStr = (a.lastCheckout instanceof Date) ? a.lastCheckout.format("yyyy-MM-dd HH:mm") : a.lastCheckout.toString()
            }
        } catch(Exception e) {
            log.warn "Analytics: Error formatting dates: ${e.message}"
        }

        def html = """
            <table style="width:100%; border-collapse: collapse;">
                <tr style="background-color: #f0f0f0;">
                    <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Metric</th>
                    <th style="padding: 8px; text-align: center; border: 1px solid #ddd;">Success</th>
                    <th style="padding: 8px; text-align: center; border: 1px solid #ddd;">Failed</th>
                    <th style="padding: 8px; text-align: center; border: 1px solid #ddd;">Rate</th>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;">Check-Ins</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd; color: green;">${a.checkinSuccess ?: 0}</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd; color: red;">${a.checkinFailed ?: 0}</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd;">${checkinRate}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;">Check-Outs</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd; color: green;">${a.checkoutSuccess ?: 0}</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd; color: red;">${a.checkoutFailed ?: 0}</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd;">${checkoutRate}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;">Lock Programming</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd; color: green;">${a.lockProgramSuccess ?: 0}</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd; color: red;">${a.lockProgramFailed ?: 0}</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd;">${lockProgramRate}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;">Lock Code Deletion</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd; color: green;">${a.lockDeleteSuccess ?: 0}</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd; color: red;">${a.lockDeleteFailed ?: 0}</td>
                    <td style="padding: 8px; text-align: center; border: 1px solid #ddd;">${lockDeleteRate}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;">Lock Retry Stats</td>
                    <td colspan="3" style="padding: 8px; border: 1px solid #ddd;">
                        Program: Avg ${programAvg} attempts, ${programFirstTryPct}% first-try<br>
                        Delete: Avg ${deleteAvg} attempts, ${deleteFirstTryPct}% first-try
                    </td>
                </tr>
            </table>
            <p><strong>Last Check-In:</strong> ${lastCheckinStr}</p>
            <p><strong>Last Check-Out:</strong> ${lastCheckoutStr}</p>
        """
        return html
    } catch(Exception e) {
        log.error "Analytics: Rendering failed: ${e.message}"
        return "<p style='color:red;'>Analytics rendering error: ${e.message}</p>"
    }
}

/**
 * Generates an HTML-formatted list of recent events for display in the UI.
 * @return HTML string with recent events
 */
private String getRecentEventsHtml() {
    try {
        initializeAnalytics()
        def events = state.analytics.recentEvents ?: []

        if (events.isEmpty()) {
            return "<p><em>No recent events recorded</em></p>"
        }

        def html = "<table style=\"width:100%; border-collapse: collapse; font-size: 0.9em;\">"
        html += "<tr style=\"background-color: #f0f0f0;\">"
        html += "<th style=\"padding: 6px; text-align: left; border: 1px solid #ddd;\">Time</th>"
        html += "<th style=\"padding: 6px; text-align: left; border: 1px solid #ddd;\">Event</th>"
        html += "<th style=\"padding: 6px; text-align: left; border: 1px solid #ddd;\">Details</th>"
        html += "</tr>"

        events.each { event ->
            def color = event.type?.contains("success") ? "green" : (event.type?.contains("failed") ? "red" : "black")
            def eventName = event.type?.replace("_", " ")?.capitalize() ?: "Unknown"
            html += "<tr>"
            html += "<td style=\"padding: 6px; border: 1px solid #ddd;\">${event.timestamp ?: ''}</td>"
            html += "<td style=\"padding: 6px; border: 1px solid #ddd; color: ${color};\">${eventName}</td>"
            html += "<td style=\"padding: 6px; border: 1px solid #ddd;\">${event.details ?: ''}</td>"
            html += "</tr>"
        }
        html += "</table>"
        return html
    } catch(Exception e) {
        log.error "Analytics: Recent events rendering failed: ${e.message}"
        return "<p style='color:red;'>Recent events rendering error: ${e.message}</p>"
    }
}

/**
 * Resets all analytics data to initial values.
 */
private void resetAnalytics() {
    state.analytics = null
    initializeAnalytics()
    log.info "Analytics: Data has been reset"
}

preferences {
    page(name: "mainPage", install: true, uninstall: true)
}

def mainPage() {
    dynamicPage(name: "mainPage", install: true, uninstall: true) {
        section {
            paragraph """<h1>Rental Automator by <a href="https://thedanhealy.com" target="_blank">Dan Healy</a></h1>"""
            paragraph """If you enjoy using this app, please consider donating to the <a href="https://brotherson3.org" target="_blank"><strong>Josh Minton Foundation</strong></a>. I created this app to help our foundation better manage the AirBNB guests, which provides us with extra funding inbetween our no-cost therapeutic stays. I am now offering this app to the community for free, but in hopes that you'll also donate back in appreciation for the free usage."""
        }
        section {
            input name: "toggleAutomation", type: "button", title: atomicState.automationEnabled ? "Disable Rental Automator" : "Enable Rental Automator"
            if(atomicState.automationEnabled) {
                paragraph "<p style=\"color:green;\">The Rental Automator is currently enabled</p>"
            } else {
                paragraph "<p style=\"color:red;\"><strong>The Rental Automator is currently disabled</strong></p>"
            }
            input name: "debugMode", type: "bool", title: "Enable Debug Mode", submitOnChange: true
        }
        section {
            paragraph "<h2>Calendar Settings</h2><hr>"
            input name: "calFormat", type: "enum", title: "Calendar Format", required: true, multiple: false, options: ["AirBNB","OwnerRez"], defaultValue: "OwnerRez"
            input name: "calendarUrl", type: "text", title: "Calendar URL", required: true
            paragraph """To learn how to obtain your AirBNB Calendar URL, click <a href="https://www.airbnb.com/help/article/99#section-heading-9-0" target="_blank">here</a>."""
            paragraph """To learn how to obtain your OwnerRez Calendar URL, click <a href="https://www.ownerrez.com/support/articles/custom-ical-links" target="_blank">here</a>."""
            input name: "testCalendarUrl", type: "button", title: "Test"
            if(atomicState.testCalendar) {
                if(atomicState.testCalendarUrlState) {
                    paragraph "<p style=\"color:green;\">Calendar URL Verified & Tested</p>"
                }
                if(!atomicState.testCalendarUrlState && atomicState.testCalendarUrlState != null) {
                    paragraph "<p style=\"color:red;\"><strong>There's an issue with the Calendar URL. Please check the calendar URL, click the Save button at the bottom of the page, then try again</strong></p>"
                }
            }
        }
        section{
            paragraph "<h2>Door Locks</h2><hr>"
            input name: "doorLocks", type: "capability.lock", title: "Which door lock(s) do you want to program", submitOnChange: true, required: true, multiple: true
            if(doorLocks) {
                for (lock in doorLocks) {
                    if (!lock.hasCommand("setCode")) paragraph "<p style=\"color:red; padding-top: 30px;\"><strong>${lock} DOES NOT SUPPORT PROGRAMMING OF CODES THROUGH HUBITAT</strong></p>"
                }
            }
        }
        section{
            paragraph "<h2>Check-In & Check-Out Settings</h2><hr>"
            input name: "checkinTime", type: "time", title: "When is default Check-In Time?", required: true, defaultValue: "16:00"
            input name: "checkoutTime", type: "time", title: "When is default Check-Out Time?", required: true, defaultValue: "11:00"
            if(calFormat == "OwnerRez") {
                input name: "useOwnerRezTimes", type: "bool", title: "Use OwnerRez times for exact booking check-in/out?", required: false
            }
            paragraph "<em>If you want to have the door lock codes programmed earlier than Check-In time, please enable the Check-In Prep.</em>"
            input name: "checkinPrep", type: "bool", title: "Do you need to run any preparations before Check-In (called \"Check-In Prep\"), such as cooling or heating?", submitOnChange: true, defaultValue: false
            if(checkinPrep) {
                input name: "checkinPrepMinutes", type: "number", title: "How many minutes before Check-In should the preparations start?", required: true
            }
            input name: "checkinEarlyMinutes", type: "number", title: "Run Check-In procedure how many minutes early? (0-60)",
                required: false, range: "0..60", defaultValue: 0
            input name: "checkoutLateMinutes", type: "number", title: "Run Check-Out procedure how many minutes late? (0-60)",
                required: false, range: "0..60", defaultValue: 0
			input name: "checkinMode", type: "mode", title: "Which mode do you want to activate for Check-In?", submitOnChange: true, required: true, defaultValue: "Stay"
            if(!checkinMode) {
                paragraph "<p style=\"color:orange;\"><strong>Warning:</strong> Check-In Mode is not set. Please select a mode above and click Done to save.</p>"
            }
            if(checkinPrep) {
                input name: "checkinPrepSameMode", type: "bool", title: "Do you want to use the same mode for Check-In Prep as Check-In?", submitOnChange: true
                if(!checkinPrepSameMode) {
                   input name: "checkinPrepMode", type: "mode", title: "Which mode do you want to activate for Check-In Prep?", submitOnChange: true, required: true
                }
				input name: "programLocksAtCheckinPrep", type: "bool", title: "Do you want to program the door lock codes at Check-In Prep time?"
            }
            input name: "checkoutMode", type: "mode", title: "Which mode do you want to activate for Check-Out?", submitOnChange: true, required: true, defaultValue: "Away"
            if(!checkoutMode) {
                paragraph "<p style=\"color:orange;\"><strong>Warning:</strong> Check-Out Mode is not set. Please select a mode above and click Done to save.</p>"
            }
        }
        section {
            paragraph "<h2>Notification Settings</h2><hr>"
            input name: "raHubName", type: "text", title: "What name do you want to use for this Hub in your notifications?", required: false
            input name: "notificationDevices", type: "capability.notification", title: "Which devices do you want to use for push notifications?", multiple: true, required: false, submitOnChange: true
            if(debugMode) input name: "sendTestNotification", type: "button", title: "Debug: Send Test Notification"
            input name: "notificationOnErrorsOnly", type: "bool", title: "Do you want to only be notified when there's error? Otherwise, a notification will be sent when each mode gets activated", submitOnChange: true
        }
        section{
            if(debugMode) {
                input name: "forceEventOverride", type: "bool", title: "Do you want to force the test procedures below to execute on the first calendar event?", submitOnChange: true
                input name: "testCheckinPrepProcedure", type: "button", title: "Test Check-In Prep Procedure"
                input name: "testCheckinProcedure", type: "button", title: "Test Check-In Procedure"
                input name: "testCheckoutProcedure", type: "button", title: "Test Check-Out Procedure"
                input name: "testDoorLockProgramming", type: "button", title: "Test Door Lock Programming"
                input name: "testPolling", type: "button", title: "Test update polling"
            }
        }
        section{
            paragraph "<h2>Analytics</h2><hr>"
            paragraph getAnalyticsSummary()
            input name: "showRecentEvents", type: "bool", title: "Show Recent Events", submitOnChange: true
            if(showRecentEvents) {
                paragraph "<h3>Recent Events (Last 20)</h3>"
                paragraph getRecentEventsHtml()
            }
            input name: "resetAnalyticsButton", type: "button", title: "Reset Analytics"
        }
    }
}

void appButtonHandler(btn) {
    debugLog "Button handler: ${btn} pressed"
    if(btn == "testCalendarUrl") {
        testCalendarUrl(calendarUrl)
    } else if(btn == "toggleAutomation") {
        atomicState.testCalendar = false
        if(atomicState.automationEnabled) {
            disableAutomation()
            atomicState.automationEnabled = false
        } else {
            enableAutomation()
            atomicState.automationEnabled = true
        }
    } else if(btn == "sendTestNotification") {
        sendNotification("This is a test notification")
    } else if(btn == "testCheckinPrepProcedure") {
        checkinPrepProcedure(forceEventOverride = true)
    } else if(btn == "testCheckinProcedure") {
        checkinProcedure(forceEventOverride = true)
    } else if(btn == "testCheckoutProcedure") {
        checkoutProcedure(forceEventOverride = true)
    } else if(btn == "testDoorLockProgramming") {
        testDoorLockProgramming()
    } else if(btn == "testPolling") {
        pollIcalForUpdates()
    } else if(btn == "resetAnalyticsButton") {
        resetAnalytics()
    }
}

// Called when app first installed
def installed() {
    log.trace "installed()"
    initializeAnalytics()
    migrateState()
    initialize()
}

// Called when user presses "Done" button in app
def updated() {
    atomicState.remove("testCalendar")
    atomicState.remove("testCalendarUrlState")
    log.trace "updated()"

    // Validate configuration and store result
    def errors = validateConfiguration()
    if(errors) {
        log.error "Settings: Configuration errors found: ${errors.join(', ')}"
        state.configurationValid = false
    } else {
        state.configurationValid = true
        debugLog "Settings: Configuration validated successfully"
    }

    // Run state migration for version updates
    migrateState()

    // Re-initialize subscriptions
    initialize()
}

// Called when app uninstalled
def uninstalled() {
    log.trace "uninstalled()"
    // Most apps would not need to do anything here
}

//--------------------------------------------------------------
// Configuration Validation
//--------------------------------------------------------------

/**
 * Validates the app configuration and returns a list of errors.
 * Called on save to catch configuration problems early.
 * @return List of error messages (empty if configuration is valid)
 */
private List<String> validateConfiguration() {
    def errors = []

    // Validate calendar URL uses HTTPS
    if(calendarUrl && !isValidSecureUrl(calendarUrl)) {
        errors << "Calendar URL must use HTTPS for security"
    }

    // Validate checkinPrepMinutes if prep is enabled
    if(checkinPrep) {
        if(!checkinPrepMinutes || checkinPrepMinutes <= 0) {
            errors << "Check-in prep minutes must be a positive number"
        }
        // Validate prep mode is set if not using same mode
        if(!checkinPrepSameMode && !checkinPrepMode) {
            errors << "Check-in prep mode must be selected when not using same mode as check-in"
        }
    }

    // Validate door locks support code programming
    doorLocks?.each { lock ->
        if(!lock.hasCommand("setCode")) {
            errors << "Lock '${lock}' does not support code programming (setCode command)"
        }
        if(!lock.hasCommand("deleteCode")) {
            errors << "Lock '${lock}' does not support code deletion (deleteCode command)"
        }
    }

    // Validate required modes are set
    if(!checkinMode) {
        errors << "Check-in mode must be selected"
    }
    if(!checkoutMode) {
        errors << "Check-out mode must be selected"
    }

    // Validate times are set
    if(!checkinTime) {
        errors << "Check-in time must be set"
    }
    if(!checkoutTime) {
        errors << "Check-out time must be set"
    }

    // Validate check-in early minutes
    if(checkinEarlyMinutes != null && (checkinEarlyMinutes < 0 || checkinEarlyMinutes > 60)) {
        errors << "Check-in early minutes must be between 0 and 60"
    }

    // Validate check-out late minutes
    if(checkoutLateMinutes != null && (checkoutLateMinutes < 0 || checkoutLateMinutes > 60)) {
        errors << "Check-out late minutes must be between 0 and 60"
    }

    return errors
}

//--------------------------------------------------------------
// State Migration
//--------------------------------------------------------------

/**
 * Migrates state data when app version changes.
 * Handles backward compatibility and data structure updates.
 *
 * Version History:
 *   Version 1: Initial release
 *   Version 2: Added analytics.recentEvents structure, pendingLockOperations
 */
private void migrateState() {
    def currentVersion = APP_VERSION
    def stateVersion = state.appVersion ?: 1

    if(stateVersion >= currentVersion) {
        debugLog "State migration: Already current (version ${stateVersion})"
        return
    }

    log.info "State migration: Migrating from version ${stateVersion} to ${currentVersion}"

    // Migration from version 1 to version 2
    if(stateVersion < 2) {
        // Ensure analytics structure has recentEvents list
        if(state.analytics && !state.analytics.recentEvents) {
            state.analytics.recentEvents = []
            debugLog "State migration: Added analytics.recentEvents structure"
        }

        // Initialize pendingLockOperations if not present
        if(!state.pendingLockOperations) {
            state.pendingLockOperations = [:]
            debugLog "State migration: Initialized pendingLockOperations"
        }

        // Ensure configurationValid is set
        if(state.configurationValid == null) {
            state.configurationValid = true
            debugLog "State migration: Set default configurationValid"
        }
    }

    // Future migrations would go here:
    // if(stateVersion < 3) { ... }

    // Update stored version
    state.appVersion = currentVersion
    log.info "State migration: Complete (now at version ${currentVersion})"
}

//--------------------------------------------------------------
// Lock Event Subscription and Verification
//--------------------------------------------------------------

/**
 * Initializes the app by subscribing to lock events for async verification.
 * Called after app is installed or updated.
 */
def initialize() {
    log.info "Initialization: Starting Rental Automator"

    // Unsubscribe from previous events
    unsubscribe()

    // Subscribe to lock code change events for verification
    doorLocks?.each { lock ->
        subscribe(lock, "codeChanged", lockCodeChangedHandler)
        debugLog "Initialization: Subscribed to events for lock: ${lock}"
    }

    // Initialize pending operations tracking
    if(!state.pendingLockOperations) {
        state.pendingLockOperations = [:]
    }
}

/**
 * Handles lock code change events for async verification.
 * Verifies that programmed/deleted codes were actually applied.
 * @param evt The lock code change event
 */
def lockCodeChangedHandler(evt) {
    debugLog "Lock event: Code change received - ${evt.device} - ${evt.value}"

    def lockName = evt.device.toString()
    def eventValue = evt.value

    // Check if we have a pending operation for this lock
    def pending = state.pendingLockOperations?.get(lockName)
    if(pending) {
        def operationType = pending.type

        if(eventValue?.contains("set") || eventValue?.contains("added")) {
            if(operationType == "program") {
                debugLog "Lock event: ${lockName} code programming confirmed"
                recordAnalyticsEvent("lock_event_confirmed", "Lock: ${lockName}, Operation: program")
            }
        } else if(eventValue?.contains("deleted") || eventValue?.contains("removed")) {
            if(operationType == "delete") {
                debugLog "Lock event: ${lockName} code deletion confirmed"
                recordAnalyticsEvent("lock_event_confirmed", "Lock: ${lockName}, Operation: delete")
            }
        } else if(eventValue?.contains("failed")) {
            log.warn "Lock event: ${lockName} operation failed - ${eventValue}"
            recordAnalyticsEvent("lock_event_failed", "Lock: ${lockName}, Value: ${eventValue}")
        }

        // Clear pending operation
        state.pendingLockOperations.remove(lockName)
    } else {
        // Unexpected code change - log for awareness
        debugLog "Lock event: ${lockName} unexpected code change - ${eventValue}"
    }
}

/**
 * Records a pending lock operation for async verification.
 * @param lockName The lock device name
 * @param operationType "program" or "delete"
 */
private void recordPendingOperation(String lockName, String operationType) {
    if(!state.pendingLockOperations) {
        state.pendingLockOperations = [:]
    }
    state.pendingLockOperations[lockName] = [
        type: operationType,
        timestamp: new Date().getTime()
    ]
    debugLog "Lock tracking: Recorded pending ${operationType} for ${lockName}"
}

//--------------------------------------------------------------
// Scheduling and automation enabling/disabling
//--------------------------------------------------------------

/**
 * Enables the rental automation by scheduling check-in, check-out, and prep procedures.
 * Delegates to format-specific scheduling methods based on configuration.
 */
def enableAutomation() {
    log.info "Automation: Enabling (format: ${calFormat}, useOwnerRezTimes: ${useOwnerRezTimes ?: false})"
    unschedule()  // Clear any existing schedules before setting new ones
    if(calFormat == "OwnerRez" && useOwnerRezTimes) {
        scheduleWithOwnerRezTimes()
    } else {
        scheduleWithDefaultTimes()
    }
}

/**
 * Gets the validated prep minutes value.
 * @return Validated prep minutes (defaults to 30 if invalid)
 */
private int getValidatedPrepMinutes() {
    if(!checkinPrep) return 0
    def prepMinutes = (checkinPrepMinutes ?: 0).toInteger()
    if(prepMinutes <= 0) {
        log.warn "Check-In Prep: Minutes must be a positive number; using default of 30"
        return 30
    }
    return prepMinutes
}

/**
 * Gets the validated check-in early minutes value.
 * @return Validated early minutes clamped to 0-60 range (defaults to 0)
 */
private int getValidatedCheckinEarlyMinutes() {
    def minutes = (checkinEarlyMinutes ?: 0).toInteger()
    return Math.max(0, Math.min(60, minutes))
}

/**
 * Gets the validated check-out late minutes value.
 * @return Validated late minutes clamped to 0-60 range (defaults to 0)
 */
private int getValidatedCheckoutLateMinutes() {
    def minutes = (checkoutLateMinutes ?: 0).toInteger()
    return Math.max(0, Math.min(60, minutes))
}

/**
 * Schedules check-in events (main check-in and optional prep).
 * @param checkinDate The Date object for check-in time
 * @param prepMinutes Minutes before check-in for prep (0 if no prep)
 * @return true if check-in time has already passed (caller should execute directly)
 */
private boolean scheduleCheckinEvents(Date checkinDate, int prepMinutes) {
    def earlyMinutes = getValidatedCheckinEarlyMinutes()
    def now = new Date()

    // Adjust check-in time earlier
    def adjustedCheckinDate = new Date(checkinDate.getTime() - (earlyMinutes * 60000))

    if(adjustedCheckinDate.before(now)) {
        // Check-in time already passed — caller will execute directly
        log.warn "Scheduling: Check-in time (${adjustedCheckinDate}) has already passed; will execute directly"
        return true
    } else {
        // Check-in is still in the future — schedule via cron
        def cronCheckin = convertDateToCron(adjustedCheckinDate)
        debugLog "Scheduling: Check-in at ${cronCheckin} (${earlyMinutes} min early)"
        schedule(cronCheckin, "checkinProcedure")

        if(checkinPrep && prepMinutes > 0) {
            // Prep is relative to ADJUSTED check-in time
            def prepDate = new Date(adjustedCheckinDate.getTime() - (prepMinutes * 60000))
            if(prepDate.before(now)) {
                log.warn "Scheduling: Check-in prep time (${prepDate}) has already passed; skipping"
            } else {
                def cronPrep = convertDateToCron(prepDate)
                debugLog "Scheduling: Check-in prep at ${cronPrep}"
                schedule(cronPrep, "checkinPrepProcedure")
            }
        }
        return false
    }
}

/**
 * Schedules check-in events using time preference (cron-based daily schedule).
 * @param prepMinutes Minutes before check-in for prep (0 if no prep)
 */
private void scheduleCheckinEventsFromTime(int prepMinutes) {
    def earlyMinutes = getValidatedCheckinEarlyMinutes()

    // Schedule check-in (adjusted earlier if configured)
    schedule(convertTimeToCron(checkinTime, earlyMinutes, 0), "checkinProcedure")

    if(checkinPrep && prepMinutes > 0) {
        // Prep must occur BEFORE the adjusted check-in time
        def totalPrepOffset = prepMinutes + earlyMinutes
        schedule(convertTimeToCron(checkinTime, totalPrepOffset, 0), "checkinPrepProcedure")
    }
}

/**
 * Schedules check-out events.
 * @param checkoutDate The Date object for check-out time
 * @return true if check-out time has already passed (caller should execute directly)
 */
private boolean scheduleCheckoutEvents(Date checkoutDate) {
    def lateMinutes = getValidatedCheckoutLateMinutes()
    def now = new Date()
    def adjustedCheckoutDate = new Date(checkoutDate.getTime() + (lateMinutes * 60000))

    if(adjustedCheckoutDate.before(now)) {
        // Check-out time already passed — caller will execute directly
        log.warn "Scheduling: Check-out time (${adjustedCheckoutDate}) has already passed; will execute directly"
        return true
    } else {
        def cronCheckout = convertDateToCron(adjustedCheckoutDate)
        debugLog "Scheduling: Check-out at ${cronCheckout} (${lateMinutes} min late)"
        schedule(cronCheckout, "checkoutProcedure")
        return false
    }
}

/**
 * Schedules check-out events using time preference (cron-based daily schedule).
 */
private void scheduleCheckoutEventsFromTime() {
    def lateMinutes = getValidatedCheckoutLateMinutes()
    schedule(convertTimeToCron(checkoutTime, 0, lateMinutes), "checkoutProcedure")
}

/**
 * Schedules automation using OwnerRez exact times from calendar.
 * Fetches calendar to get exact check-in/check-out times from booking.
 * Falls back to default times if no events found for today.
 */
private void scheduleWithOwnerRezTimes() {
    log.info "Scheduling: Using OwnerRez exact times"
    def prepMinutes = getValidatedPrepMinutes()
    def iCalDict = fetchICalData(calendarUrl)
    if(!iCalDict) {
        log.warn "OwnerRez scheduling: Calendar data unavailable; falling back to default times"
        scheduleWithDefaultTimes()
        return
    }
    def todaysDate = new Date().format("yyyyMMdd")

    // Schedule check-out FIRST (must run before check-in when both are past-due)
    def checkoutEvent = iCalDict.find { event ->
        extractDateFromICalDate(event.endDate) == todaysDate
    }
    def checkoutPastDue = false
    if(checkoutEvent) {
        def checkoutDate = parseICalDate(checkoutEvent.endDate)
        if(!checkoutDate) {
            log.error "OwnerRez scheduling: Failed to parse check-out date: ${checkoutEvent.endDate}"
            scheduleCheckoutEventsFromTime()
        } else {
            state.ownerRezCheckoutDate = checkoutDate
            debugLog "OwnerRez scheduling: Found check-out event for today"
            checkoutPastDue = scheduleCheckoutEvents(checkoutDate)
        }
    } else {
        debugLog "OwnerRez scheduling: No check-out event found; using settings value"
        scheduleCheckoutEventsFromTime()
    }

    // Schedule check-in SECOND
    def checkinEvent = iCalDict.find { event ->
        extractDateFromICalDate(event.startDate) == todaysDate
    }
    def checkinPastDue = false
    if(checkinEvent) {
        def checkinDate = parseICalDate(checkinEvent.startDate)
        if(!checkinDate) {
            log.error "OwnerRez scheduling: Failed to parse check-in date: ${checkinEvent.startDate}"
            scheduleCheckinEventsFromTime(prepMinutes)
        } else {
            state.ownerRezCheckinDate = checkinDate
            debugLog "OwnerRez scheduling: Found check-in event for today"
            checkinPastDue = scheduleCheckinEvents(checkinDate, prepMinutes)
        }
    } else {
        debugLog "OwnerRez scheduling: No check-in event found; using settings value"
        scheduleCheckinEventsFromTime(prepMinutes)
    }

    // Schedule polling for updated iCal times every 15 minutes
    schedule("0 0/15 * ? * *", "pollIcalForUpdates")

    // Execute past-due procedures directly (synchronous, guaranteed execution)
    // Order matters: checkout must complete before checkin starts
    if(checkoutPastDue) {
        log.info "Scheduling: Running past-due check-out procedure now"
        checkoutProcedure()
    }
    if(checkinPastDue) {
        log.info "Scheduling: Running past-due check-in procedure now"
        checkinProcedure()
    }
}

/**
 * Schedules automation using default times from settings.
 * Used for AirBNB and OwnerRez without exact times enabled.
 */
private void scheduleWithDefaultTimes() {
    log.info "Scheduling: Using default times"
    def prepMinutes = getValidatedPrepMinutes()
    scheduleCheckinEventsFromTime(prepMinutes)
    scheduleCheckoutEventsFromTime()
}

/**
 * Disables all scheduled automation by unscheduling all jobs.
 */
def disableAutomation() {
    log.info "Automation: Disabling all schedules"
    unschedule()
}

/**
 * Polls the iCal feed for updated check-in/check-out times.
 * Used for OwnerRez when exact times may change (e.g., early check-in granted).
 * Only active between 30 minutes before checkout and the scheduled check-in time.
 * If times have changed, reschedules all automation with the new times.
 */
def pollIcalForUpdates() {
    if (!(calFormat == "OwnerRez" && useOwnerRezTimes)) {
        debugLog "iCal polling: Not using OwnerRez times; skipping"
        return
    }

    def checkoutDate = state.ownerRezCheckoutDate
    def checkinDate = state.ownerRezCheckinDate
    def now = new Date()

    if (!checkoutDate || !checkinDate) {
        debugLog "iCal polling: Stored times not available; skipping"
        return
    }

    // Polling starts 30 minutes prior to the stored checkout time
    def pollStartTime = new Date(checkoutDate.getTime() - (30 * 60000))

    if (!now.after(pollStartTime) || !now.before(checkinDate)) {
        debugLog "iCal polling: Outside window (${now} not between ${pollStartTime} and ${checkinDate}); skipping"
        return
    }

    debugLog "iCal polling: Checking for updated times"

    def iCalDict = fetchICalData(calendarUrl)
    if(!iCalDict) {
        log.warn "iCal polling: Calendar data unavailable; skipping poll"
        return
    }
    def todaysDate = now.format("yyyyMMdd")

    // Try to locate updated events using helper function
    def updatedCheckoutEvent = iCalDict.find { event ->
        extractDateFromICalDate(event.endDate) == todaysDate
    }
    def updatedCheckinEvent = iCalDict.find { event ->
        extractDateFromICalDate(event.startDate) == todaysDate
    }

    if (!updatedCheckinEvent && !updatedCheckoutEvent) {
        debugLog "iCal polling: No valid updated events found; skipping"
        return
    }

    // If an event is missing, retain the stored value
    def newCheckinDate = updatedCheckinEvent ? parseICalDate(updatedCheckinEvent.startDate) : state.ownerRezCheckinDate
    def newCheckoutDate = updatedCheckoutEvent ? parseICalDate(updatedCheckoutEvent.endDate) : state.ownerRezCheckoutDate

    // Only update and re-schedule if at least one of the times has changed
    if (newCheckinDate?.getTime() == state.ownerRezCheckinDate?.getTime() &&
        newCheckoutDate?.getTime() == state.ownerRezCheckoutDate?.getTime()) {
        debugLog "iCal polling: No change in times from last poll"
        return
    }

    debugLog "iCal polling: Updated times found. Old checkout: ${state.ownerRezCheckoutDate}, New checkout: ${newCheckoutDate}; Old checkin: ${state.ownerRezCheckinDate}, New checkin: ${newCheckinDate}"

    state.ownerRezCheckoutDate = newCheckoutDate
    state.ownerRezCheckinDate = newCheckinDate

    // Re-schedule automation with the updated times
    log.info "iCal polling: Times changed, rescheduling automation"
    disableAutomation()
    enableAutomation()
}

/**
 * Converts a Hubitat time preference value to a cron expression.
 * Optionally adjusts the time by adding or subtracting minutes.
 * @param timeVar Time string from Hubitat time input (format: yyyy-MM-dd'T'HH:mm:ss.SSSZ)
 * @param minutesToSubtract Minutes to subtract from the time (default: 0)
 * @param minutesToAdd Minutes to add to the time (default: 0)
 * @return Cron expression string (e.g., "0 30 15 * * ?")
 */
def convertTimeToCron(timeVar, minutesToSubtract = 0, minutesToAdd = 0) {
    debugLog "Cron conversion: Converting time preference"
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    Calendar calendar = new GregorianCalendar()
    Date timeVarObj = format.parse(timeVar)
    calendar.setTime(timeVarObj)
    calendar.add(Calendar.MINUTE, (-1 * minutesToSubtract.toInteger()) )
	calendar.add(Calendar.MINUTE, (minutesToAdd.toInteger()) )
    timeVarObj = calendar.getTime()
    debugLog "Cron conversion: Adjusted time: " + timeVarObj
    String hour = calendar.get(Calendar.HOUR_OF_DAY).toString()
    String minute = calendar.get(Calendar.MINUTE).toString()
    String cronExp = "0 ${minute} ${hour} * * ?"
    return cronExp
}

/**
 * Converts a Date object to a cron expression for scheduling.
 * @param date The Date object to convert
 * @return Cron expression string (e.g., "0 30 15 * * ?")
 */
def convertDateToCron(Date date) {
    def cal = Calendar.getInstance()
    cal.setTime(date)
    String hour = cal.get(Calendar.HOUR_OF_DAY).toString()
    String minute = cal.get(Calendar.MINUTE).toString()
    String cronExp = "0 ${minute} ${hour} * * ?"
    debugLog "Cron conversion: Date to expression: ${cronExp}"
    return cronExp
}

/**
 * Sends a push notification to all configured notification devices.
 * Prepends "Rental Automator" (and optional hub name) to the message.
 * @param msg The notification message to send
 */
void sendNotification(msg) {
    notificationDevices?.each { device ->
        debugLog "Notification: Sending to ${device}"
        try {
            if(raHubName) {
                device.deviceNotification("Rental Automator (${raHubName}): ${msg}")
            } else {
                device.deviceNotification("Rental Automator: ${msg}")
            }
            debugLog "Notification: Sent successfully to ${device}"
        } catch (Exception deviceError) {
            log.error "Notification: Failed to send to ${device}: ${deviceError.message}"
        }
    }
}

//--------------------------------------------------------------
// Calendar Functions (and helpers)
//--------------------------------------------------------------

/**
 * Fetches and parses iCal data from the configured calendar URL.
 * Includes security validation (HTTPS), rate limiting, and fallback to cached data.
 * @param calendarUrl The iCal calendar URL to fetch
 * @param forceFetch If true, bypasses rate limiting (used for manual testing)
 * @return List of parsed calendar events as maps
 */
def fetchICalData(calendarUrl, Boolean forceFetch = false) {
    def iCalDict = null

    // Security: Validate URL uses HTTPS
    if(!isValidSecureUrl(calendarUrl)) {
        log.error "Calendar fetch: URL must use HTTPS. Skipping."
        atomicState.testCalendarUrlState = false
        if(state.iCalDictPrevious) {
            debugLog "Calendar fetch: Returning cached data due to invalid URL"
            return state.iCalDictPrevious
        }
        return null
    }

    // Rate limiting: Check if we can fetch (unless forced)
    if(!forceFetch && !canFetchCalendar() && !debugMode) {
        debugLog "Calendar fetch: Rate limited, using cached data"
        if(state.iCalDictPrevious) {
            return state.iCalDictPrevious
        }
    }

	try {
        def iCalData = getCalendarData(calendarUrl)

        // Validate that we received actual iCal data
        if(!iCalData || iCalData.trim().isEmpty()) {
            log.error "Calendar fetch: Empty response. Check URL is valid and accessible."
            atomicState.testCalendarUrlState = false
        } else if(!iCalData.contains("VCALENDAR")) {
            log.error "Calendar fetch: Invalid response, does not contain VCALENDAR"
            atomicState.testCalendarUrlState = false
        } else {
            iCalDict = iCalToMapList(iCalData)
            state.iCalDictPrevious = iCalDict
            atomicState.testCalendarUrlState = true
            recordCalendarFetch()  // Record successful fetch time for rate limiting
        }
    } catch (groovyx.net.http.HttpResponseException e) {
        log.error "Calendar fetch: HTTP request failed"
        atomicState.testCalendarUrlState = false
    } catch (Exception e) {
        log.error "Calendar fetch: Failed to parse iCalendar data. ${e}"
        atomicState.testCalendarUrlState = false
    }
	if(atomicState.testCalendarUrlState == false){
		debugLog "Calendar fetch: Failed, checking for cached data"
		if(state.iCalDictPrevious) {
			iCalDict = state.iCalDictPrevious
			debugLog "Calendar fetch: Reverting to previous cached data"
		}
		else {
			log.error "Calendar fetch: Unable to read iCal URL and no cached copy. Check-in and check-out operations cannot proceed."
			sendNotification "Unable to read iCal URL and no previous copy. Therefore unable to perform check-in or check-out operations"
		}
	}
	debugLog "Calendar fetch: Retrieved ${iCalDict?.size() ?: 0} events"
	return iCalDict
}

/**
 * Tests the calendar URL by fetching and parsing iCal data.
 * Validates that the URL uses HTTPS before attempting to fetch.
 * @param calendarUrl The iCal URL to test
 */
def testCalendarUrl(calendarUrl) {
    atomicState.testCalendar = true
    log.info "Calendar test: Testing iCalendar URL"
    if(!calendarUrl) {
        log.info "Calendar test: URL must be saved first. Please enter all required settings, save, then try again."
        atomicState.testCalendarUrlState = false
        return
    }
    // Security: Validate URL uses HTTPS
    if(!isValidSecureUrl(calendarUrl)) {
        log.error "Calendar test: URL must use HTTPS. Please update."
        atomicState.testCalendarUrlState = false
        return
    }
	fetchICalData(calendarUrl, true)  // Force fetch for testing, bypass rate limit
	return
}

/**
 * Performs HTTP GET request to fetch raw iCal data from calendar URL.
 * Note: URL is not logged to prevent exposure of authentication tokens.
 * @param calendarUrl The iCal calendar URL
 * @return Raw iCal data as string
 */
def getCalendarData(calendarUrl) {
    debugLog "Calendar fetch: Fetching from configured URL"
    httpGet(calendarUrl) { resp ->
        if (resp.data) {
            // Convert InputStream to String if needed:
            resp.data = (resp.data instanceof String ? resp.data : resp.data?.text ?: "")
            def iCalData = resp.data?.trim()
            if(debugMode) {
                // Security: Don't log raw iCal data as it may contain sensitive guest information
                log.debug "Calendar fetch: Retrieved iCal data (${iCalData?.length() ?: 0} characters)"
            }
            return iCalData
        }
    }
}

/**
 * Parses raw iCal data string into a list of event maps.
 * Uses format-specific parsing functions based on calendar format setting.
 *
 * @param str Raw iCal data string
 * @return List of event maps with keys: summary, startDate, endDate, door, name
 */
def iCalToMapList(str) {
    def events = parseCalendarData(str, calFormat)
    debugLog "iCal parsing: Parsed ${events.size()} events using ${calFormat} parser"
    return events
}

/**
 * Extracts a property value from iCalendar event text using regex.
 * Searches for pattern "propertyName...:<value>" and returns the value.
 * @param eventText The raw VEVENT text block
 * @param propertyName The property name to search for
 * @return The extracted property value, or null if not found
 */
def extractProperty(eventText, propertyName) {
    def pattern = Pattern.compile(".*${propertyName}.*?:\\h*(\\w*)\\b*")
    def matcher = pattern.matcher(eventText)
    if (matcher.find()) {
        String result = matcher.group(1)
        String trimmedResult = result.replace("\\n", "").trim()
        return trimmedResult
    } else {
        debugLog "iCal parsing: Property ${propertyName} not found"
        return null
    }
}

/**
 * Parses an iCal datetime string into a Java Date object.
 * Expected format: yyyyMMdd'T'HHmmss (e.g., 20240115T150000)
 * @param dateStr The iCal datetime string
 * @return Parsed Date object, or null on parse error
 */
def parseICalDate(dateStr) {
    def dateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss")
    try {
        def dateData = dateFormat.parse(dateStr)
        return dateData
    } catch (Exception e) {
        log.error "iCal parsing: Error parsing date/time: ${e.message}"
        return null
    }
}

//--------------------------------------------------------------
// Calendar event matching helper functions
//--------------------------------------------------------------

/**
 * Finds all check-in events for today in the calendar data.
 * Supports multiple bookings starting on the same day.
 * For AirBNB: looks for events with SUMMARY="Reserved"
 * For OwnerRez: looks for events with STATUS="CONFIRMED"
 * @param iCalDict List of parsed calendar events
 * @param forceOverride If true, returns all matching events regardless of date
 * @return List of maps containing [door: doorCode, name: guestName] for each valid event
 */
def findAllCheckinEvents(iCalDict, forceOverride) {
    def expectedSummary = getExpectedEventSummary()
    def todaysDate = new Date().format('yyyyMMdd')
    def events = []

    debugLog "Check-In search: Looking for events. Summary: '${expectedSummary}', date: '${todaysDate}', force: ${forceOverride}, total: ${iCalDict?.size() ?: 0}"

    iCalDict.each { event ->
        def eventDate = event.startDate ?: ""
        def eventDateOnly = extractDateFromICalDate(eventDate)

        def dateMatch = forceOverride || eventDateOnly == todaysDate
        def summaryMatch = event.summary == expectedSummary
        def hasDoor = event.door != null
        def validCode = event.door ? isValidDoorCode(event.door) : false

        if (dateMatch) {
            if (summaryMatch && hasDoor && validCode) {
                debugLog "Check-In search: Matched booking - start=${eventDate}, door=${maskCode(event.door)}, name=${sanitizeGuestName(event.name)}"
                events << [door: event.door, name: event.name]
            }
        }
    }

    if (events.isEmpty()) {
        debugLog "Check-In search: No bookings starting today"
    } else {
        debugLog "Check-In search: Found ${events.size()} booking(s) starting today"
    }
    return events
}


/**
 * Finds all check-out events for today in the calendar data.
 * Supports multiple bookings ending on the same day.
 * For AirBNB: looks for events with SUMMARY="Reserved"
 * For OwnerRez: looks for events with STATUS="CONFIRMED"
 * @param iCalDict List of parsed calendar events
 * @param forceOverride If true, returns all matching events regardless of date
 * @return List of matching events (may be empty)
 */
def findAllCheckoutEvents(iCalDict, forceOverride) {
    def expectedSummary = getExpectedEventSummary()
    def todaysDate = new Date().format('yyyyMMdd')
    def events = []

    iCalDict.each { event ->
        def eventDate = event.endDate ?: ""
        def eventDateOnly = extractDateFromICalDate(eventDate)
        if (forceOverride || eventDateOnly == todaysDate) {
            if (event.summary == expectedSummary) {
                debugLog "Check-Out search: Found booking ending today. end=${eventDate}"
                events << event
            }
        }
    }

    if (events.isEmpty()) {
        debugLog "Check-Out search: No bookings ending today"
    } else {
        debugLog "Check-Out search: Found ${events.size()} booking(s) ending today"
    }
    return events
}


//--------------------------------------------------------------
// Lock control helper: retry logic
//--------------------------------------------------------------

/**
 * Generic retry wrapper for lock operations.
 * Executes an operation with retry logic, pausing between attempts.
 * @param operationName Name of the operation for logging/analytics
 * @param operation Closure that performs the operation
 * @param verifySuccess Closure that returns true if operation succeeded
 * @param maxRetries Maximum number of retry attempts (default: 3)
 * @param retryDelayMs Delay between retries in milliseconds (default: 10000)
 * @return true if successful, false after all retries exhausted
 */
private Boolean attemptWithRetry(String operationName, Closure operation, Closure verifySuccess, int maxRetries = 3, int retryDelayMs = 10000) {
    for(int retry = 0; retry < maxRetries; retry++) {
        def operationResult = operation.call()
        if(operationResult == false) {
            // Operation indicated it cannot proceed (e.g., no available slot)
            log.warn "Retry: ${operationName} cannot proceed; stopping"
            break
        }
        pauseExecution(retryDelayMs)
        if(verifySuccess.call()) {
            def attemptNum = retry + 1
            log.info "Retry: ${operationName} succeeded on attempt ${attemptNum}"
            recordAnalyticsEvent("${operationName}_success", "Attempt: ${attemptNum}")
            recordRetryStats(operationName, attemptNum, true)
            return true
        }
        debugLog "Retry: ${operationName} attempt ${retry + 1}"
    }
    log.error "Retry: ${operationName} failed after ${maxRetries} attempts"
    recordAnalyticsEvent("${operationName}_failed", "Retries: ${maxRetries}")
    recordRetryStats(operationName, maxRetries, false)
    return false
}

/**
 * Records retry statistics for lock operations.
 * @param operationName "lock_program" or "lock_delete"
 * @param attempts Number of attempts made
 * @param success Whether the operation ultimately succeeded
 */
private void recordRetryStats(String operationName, int attempts, boolean success) {
    initializeAnalytics()

    if(operationName == "lock_program") {
        state.analytics.lockProgramRetryTotal = (state.analytics.lockProgramRetryTotal ?: 0) + attempts
        if(success && attempts == 1) {
            state.analytics.lockProgramFirstTry = (state.analytics.lockProgramFirstTry ?: 0) + 1
        }
    } else if(operationName == "lock_delete") {
        state.analytics.lockDeleteRetryTotal = (state.analytics.lockDeleteRetryTotal ?: 0) + attempts
        if(success && attempts == 1) {
            state.analytics.lockDeleteFirstTry = (state.analytics.lockDeleteFirstTry ?: 0) + 1
        }
    }
}

/**
 * Attempts to program a door lock code with retry logic.
 * Will retry up to 3 times if programming fails.
 * @param lock The lock device to program
 * @param code The door code to program
 * @param guestname The guest name for the code
 * @return true if successful, false after all retries exhausted
 */
def attemptProgramLock(lock, code, guestname) {
    def programmedPosition = null

    def result = attemptWithRetry(
        "lock_program",
        {
            def nextPos = findNextAvailableCodePosition(lock)
            if(!nextPos) {
                debugLog "Lock programming: No available code position on ${lock}"
                return false  // Signal to stop retrying
            }
            programmedPosition = nextPos
            debugLog "Lock programming: ${lock} at position ${nextPos}, code ${maskCode(code?.toString())}, guest ${sanitizeGuestName(guestname?.toString())}"
            programDoorLockCode(lock, nextPos, code.toString(), guestname.toString())
            return true
        },
        {
            def success = findExistingAutomatorCodePosition(lock) != null
            if(success) {
                log.info "Lock programming: ${lock} confirmed at position ${programmedPosition}"
            }
            return success
        },
        3,      // maxRetries
        10000   // retryDelayMs — Yale locks need ~10s to process setCode over Z-Wave
    )

    if(!result) {
        log.error "Lock programming: Failed for ${lock} after retries"
    }
    return result
}

/**
 * Attempts to delete a RentalAutomator code from a door lock with retry logic.
 * Will retry up to 3 times if deletion fails.
 * @param lock The lock device to delete the code from
 * @return true if successful, false after all retries exhausted
 */
def attemptDeleteLock(lock) {
    def existingCodePos = findExistingAutomatorCodePosition(lock)

    // No code found to delete - this is success (nothing to delete)
    if(!existingCodePos) {
        debugLog "Lock deletion: ${lock} - no existing code found, nothing to delete"
        recordAnalyticsEvent("lock_delete_success", "Lock: ${lock} (no code found)")
        return true
    }

    def result = attemptWithRetry(
        "lock_delete",
        {
            def codePos = findExistingAutomatorCodePosition(lock)
            if(!codePos) {
                return true  // Already deleted, will pass verification
            }
            debugLog "Lock deletion: ${lock} at position ${codePos}"
            deleteDoorLockCode(lock, codePos)
            return true
        },
        {
            def success = findExistingAutomatorCodePosition(lock) == null
            if(success) {
                log.info "Lock deletion: ${lock} confirmed successful"
            }
            return success
        }
    )

    if(!result) {
        log.error "Lock deletion: Failed for ${lock} after retries"
    }
    return result
}
		

//--------------------------------------------------------------
// Process check-in events (used by both checkinProcedure and checkinPrepProcedure)
//--------------------------------------------------------------

/**
 * Core check-in processing logic shared by checkinProcedure and checkinPrepProcedure.
 * Sets the Hubitat mode and optionally programs door lock codes.
 * Supports multiple bookings starting on the same day.
 * @param modeToSet The Hubitat mode to activate
 * @param forceOverride If true, processes all events regardless of date
 * @param programLock If true, programs the door lock with the booking code(s)
 */
def processCheckinEvent(String modeToSet, Boolean forceOverride, Boolean programLock) {
    // Warn if mode is not set
    if(!modeToSet) {
        log.warn "Check-In: Mode is not configured. Please select a mode in app settings."
        sendNotification "Warning: Check-in mode not configured"
    }

    def iCalDict = fetchICalData(calendarUrl)
    def checkinEvents = findAllCheckinEvents(iCalDict, forceOverride)

    if(checkinEvents.isEmpty()) {
        debugLog "Check-In: No valid event found for today"
        log.info "Check-In: No check-in scheduled for today"
        return
    }

    log.info "Check-In: Running procedure for mode ${modeToSet ?: 'NONE'} with ${checkinEvents.size()} booking(s)"

    // Warn if multiple check-in events found for today (may indicate overlapping bookings)
    if(checkinEvents.size() > 1) {
        log.warn "Check-In: Multiple events (${checkinEvents.size()}) found for today - may indicate overlapping bookings"
        sendNotification "Warning: ${checkinEvents.size()} check-in events found for today"
        recordAnalyticsEvent("multiple_checkins_warning", "Count: ${checkinEvents.size()}")
    }

    location.setMode(modeToSet)
    log.info "Check-In: Mode set to ${modeToSet}"

    if(!programLock) {
        recordAnalyticsEvent("checkin_success", "Mode: ${modeToSet} (no lock programming)")
        if(!notificationOnErrorsOnly) {
            sendNotification "Successfully ran the Check-In procedure for mode ${modeToSet}"
        }
        return
    }

    def allLocksSuccessful = true
    def totalBookings = checkinEvents.size()
    def processedBookings = 0

    // Process each booking's door code
    checkinEvents.each { booking ->
        processedBookings++
        def bookingLabel = totalBookings > 1 ? " (booking ${processedBookings}/${totalBookings})" : ""

        doorLocks.each { lock ->
            if(!attemptProgramLock(lock, booking.door, booking.name)) {
                log.error "Check-In: Programming lock ${lock} failed${bookingLabel}"
                sendNotification "Check-In procedure failed for ${lock}${bookingLabel}"
                allLocksSuccessful = false
            } else {
                log.info "Check-In: Lock ${lock} programmed successfully${bookingLabel}"
            }
        }
    }

    // Summary notification and analytics
    if(allLocksSuccessful) {
        log.info "Check-In: Procedure completed successfully"
        recordAnalyticsEvent("checkin_success", "Mode: ${modeToSet}, Bookings: ${totalBookings}")
        if(!notificationOnErrorsOnly) {
            def msg = totalBookings > 1 ?
                "Successfully ran the Check-In procedure for ${totalBookings} bookings" :
                "Successfully ran the Check-In procedure for mode ${modeToSet}"
            sendNotification msg
        }
    } else {
        recordAnalyticsEvent("checkin_failed", "Lock programming failed, Bookings: ${totalBookings}")
    }
}

//--------------------------------------------------------------
// Check-In and Check-In Prep procedures
//--------------------------------------------------------------

/**
 * RunIn-safe wrapper for checkinProcedure (no default params to avoid overload resolution issues).
 */
def runCheckinProcedure() {
    checkinProcedure()
}

/**
 * RunIn-safe wrapper for checkoutProcedure (no default params to avoid overload resolution issues).
 */
def runCheckoutProcedure() {
    checkoutProcedure()
}

/**
 * Main check-in procedure called at scheduled check-in time.
 * Sets the check-in mode and programs door locks (unless programLocksAtCheckinPrep is true).
 * @param forceEventOverride If true, processes first event regardless of date (for testing)
 */
def checkinProcedure(forceEventOverride = false) {
    try {
        debugLog "Check-In: Procedure called. checkinMode='${checkinMode}', checkoutMode='${checkoutMode}'"

        // Validate mode is set
        if(!checkinMode) {
            log.error "Check-In: Mode is not configured! Please go to app settings, select a mode, and click Done to save."
            sendNotification "Check-In failed: Mode not configured. Please check app settings."
            return
        }

        processCheckinEvent(checkinMode, forceEventOverride, (programLocksAtCheckinPrep != true))
    } catch(Exception e) {
        log.error "Check-In: Error in procedure: ${e}"
        sendNotification "Failed to run the Check-In procedure due to error"
        recordAnalyticsEvent("checkin_failed", "Exception: ${e.message}")
    }
}

/**
 * Check-in preparation procedure called before check-in time.
 * Sets the prep mode (for HVAC pre-conditioning, etc.) and optionally programs door locks early.
 * @param forceEventOverride If true, processes first event regardless of date (for testing)
 */
def checkinPrepProcedure(forceEventOverride = false) {
    try {
        // When in prep, use the prep mode (or same as checkinMode) and optionally program the locks.
        def prepMode = checkinPrepSameMode ? checkinMode : checkinPrepMode
        debugLog "Check-In Prep: Procedure called. prepMode='${prepMode}', sameMode=${checkinPrepSameMode}"

        // Validate mode is set
        if(!prepMode) {
            log.error "Check-In Prep: Mode is not configured! Please go to app settings, select a mode, and click Done to save."
            sendNotification "Check-In Prep failed: Mode not configured. Please check app settings."
            return
        }

        processCheckinEvent(prepMode, forceEventOverride, (programLocksAtCheckinPrep))
    } catch(Exception e) {
        log.error "Check-In Prep: Error in procedure: ${e}"
        sendNotification "Failed to run the Check-In Prep procedure due to error"
        recordAnalyticsEvent("checkin_failed", "Prep Exception: ${e.message}")
    }
}

//--------------------------------------------------------------
// Check-Out procedure
//--------------------------------------------------------------

/**
 * Main check-out procedure called at scheduled check-out time.
 * Sets the check-out mode and deletes all RentalAutomator codes from door locks.
 * Supports multiple bookings ending on the same day.
 * Includes safety check to remove any duplicate codes that shouldn't exist.
 * @param forceEventOverride If true, processes all events regardless of date (for testing)
 */
def checkoutProcedure(forceEventOverride = false) {
    try {
        debugLog "Check-Out: Procedure called. checkoutMode='${checkoutMode}'"

        // Validate mode is set
        if(!checkoutMode) {
            log.error "Check-Out: Mode is not configured! Please go to app settings, select a mode, and click Done to save."
            sendNotification "Check-Out failed: Mode not configured. Please check app settings."
            return
        }

        def iCalDict = fetchICalData(calendarUrl)
        def checkoutEvents = findAllCheckoutEvents(iCalDict, forceEventOverride)

        if(checkoutEvents.isEmpty()) {
            debugLog "Check-Out: No valid event found for today"
            log.info "Check-Out: No check-out scheduled for today"
            return
        }

        def totalCheckouts = checkoutEvents.size()
        log.info "Check-Out: Running procedure for ${totalCheckouts} booking(s)"

        // Warn if multiple check-out events found for today (may indicate overlapping bookings)
        if(checkoutEvents.size() > 1) {
            log.warn "Check-Out: Multiple events (${checkoutEvents.size()}) found for today - may indicate overlapping bookings"
            sendNotification "Warning: ${checkoutEvents.size()} check-out events found for today"
            recordAnalyticsEvent("multiple_checkouts_warning", "Count: ${checkoutEvents.size()}")
        }

        location.setMode(checkoutMode)
        log.info "Check-Out: Mode set to ${checkoutMode}"

        def allLocksSuccessful = true

        // Delete all RentalAutomator codes from all locks
        doorLocks.each { lock ->
            // Delete all codes (handles multiple bookings)
            def deletedCount = 0
            while(findExistingAutomatorCodePosition(lock) != null) {
                if(!attemptDeleteLock(lock)) {
                    log.error "Check-Out: Deleting lock ${lock} failed after retries"
                    sendNotification "Check-Out procedure failed for ${lock}"
                    allLocksSuccessful = false
                    break
                }
                deletedCount++
                // Safety limit to prevent infinite loop
                if(deletedCount >= 10) {
                    log.warn "Check-Out: Deleted ${deletedCount} codes from ${lock}, stopping to prevent infinite loop"
                    break
                }
            }
            if(deletedCount > 0) {
                log.info "Check-Out: Deleted ${deletedCount} code(s) from ${lock}"
            }
        }

        // Validate no duplicate codes remain (conflict detection)
        pauseExecution(10000)
        doorLocks.each { lock ->
            def remainingCodes = findAllAutomatorCodePositions(lock)
            if(!remainingCodes.isEmpty()) {
                log.warn "Check-Out: Found ${remainingCodes.size()} lingering code(s) on ${lock} - cleaning up"
                remainingCodes.each { pos ->
                    deleteDoorLockCode(lock, pos)
                }
            }
        }

        // Schedule safety cleanup 1 hour from now as a failsafe
        scheduleSafetyCleanup()

        // Summary and analytics
        if(allLocksSuccessful) {
            log.info "Check-Out: Procedure completed successfully"
            recordAnalyticsEvent("checkout_success", "Mode: ${checkoutMode}, Bookings: ${totalCheckouts}")
            if(!notificationOnErrorsOnly) {
                def msg = totalCheckouts > 1 ?
                    "Successfully ran the Check-Out procedure for ${totalCheckouts} bookings" :
                    "Successfully ran the Check-Out procedure"
                sendNotification msg
            }
        } else {
            log.error "Check-Out: Lock deletion failed for one or more locks"
            recordAnalyticsEvent("checkout_failed", "Lock deletion failed, Bookings: ${totalCheckouts}")
            // Still schedule safety cleanup even on failure
            log.info "Check-Out: Safety cleanup scheduled for 1 hour from now"
        }

    } catch (Exception e) {
        log.error "Check-Out: Error in procedure: ${e}"
        sendNotification "Failed to run the Check-Out procedure due to error"
        recordAnalyticsEvent("checkout_failed", "Exception: ${e.message}")
        // Schedule safety cleanup even on exception
        scheduleSafetyCleanup()
    }
}

/**
 * Schedules a safety code cleanup to run 1 hour from now.
 * This ensures codes are removed even if the checkout procedure fails.
 */
private void scheduleSafetyCleanup() {
    def cleanupTime = new Date(new Date().getTime() + (1 * 60 * 60 * 1000))  // 1 hour from now
    def cronCleanup = convertDateToCron(cleanupTime)
    debugLog "Safety cleanup: Scheduling at ${cronCleanup}"
    schedule(cronCleanup, "safetyCodeCleanup")
}

/**
 * Safety cleanup procedure that removes any lingering RentalAutomator codes.
 * Called 1 hour after checkout as a failsafe.
 * Only removes codes if no check-in is scheduled for today.
 */
def safetyCodeCleanup() {
    log.info "Safety cleanup: Running"

    try {
        // Check if there's a check-in today - don't clean up if guests arriving
        def iCalDict = fetchICalData(calendarUrl)
        def todaysCheckins = findAllCheckinEvents(iCalDict, false)
        if(!todaysCheckins.isEmpty()) {
            debugLog "Safety cleanup: Check-in scheduled for today, skipping"
            return
        }

        def codesRemoved = 0
        doorLocks.each { lock ->
            def positions = findAllAutomatorCodePositions(lock)
            if(!positions.isEmpty()) {
                log.warn "Safety cleanup: Found ${positions.size()} lingering code(s) on ${lock}"
                positions.each { pos ->
                    deleteDoorLockCode(lock, pos)
                    codesRemoved++
                }
            }
        }

        if(codesRemoved > 0) {
            log.info "Safety cleanup: Removed ${codesRemoved} lingering code(s)"
            recordAnalyticsEvent("safety_cleanup_success", "Codes removed: ${codesRemoved}")
            sendNotification "Safety cleanup removed ${codesRemoved} lingering door code(s)"
        } else {
            debugLog "Safety cleanup: No lingering codes found"
            recordAnalyticsEvent("safety_cleanup_success", "No codes to remove")
        }
    } catch (Exception e) {
        log.error "Safety cleanup: Error - ${e}"
        recordAnalyticsEvent("safety_cleanup_failed", "Exception: ${e.message}")
    }
}

//--------------------------------------------------------------
// Lock control functions
//--------------------------------------------------------------

/**
 * Finds an existing RentalAutomator code position on a lock.
 * Returns the first found position, or null if no RentalAutomator code exists.
 * @param lock The lock device to search
 * @return Code position (integer) if found, null otherwise
 */
def findExistingAutomatorCodePosition(lock) {
    def positions = findAllAutomatorCodePositions(lock)
    return positions.isEmpty() ? null : positions[0]
}

/**
 * Finds all RentalAutomator code positions on a lock.
 * Used for conflict detection when multiple codes exist (shouldn't happen normally).
 * @param lock The lock device to search
 * @return List of code positions (integers) where RentalAutomator codes exist
 */
def findAllAutomatorCodePositions(lock) {
    debugLog "Code search: Looking for RentalAutomator codes on ${lock}"
    def codes = lock.currentValue("lockCodes", true)
    if(!codes) {
        log.warn "Code search: ${lock} returned null lockCodes (lock may be offline)"
        return []
    }
    def codeJson = new JsonSlurper().parseText(codes)
    def positions = codeJson.findAll { pos, data ->
        data?.name?.toString()?.contains("RentalAutomator")
    }.collect { pos, data -> pos.toInteger() }
    debugLog "Code search: Found ${positions.size()} RentalAutomator code(s) on ${lock}"
    return positions
}

/**
 * Validates lock state and detects conflicts (multiple RentalAutomator codes).
 * Logs warnings and sends notifications if duplicate codes are found.
 * @param lock The lock device to validate
 * @return true if lock state is valid (0 or 1 code), false if conflicts detected
 */
def validateLockState(lock) {
    def positions = findAllAutomatorCodePositions(lock)
    if(positions.size() > 1) {
        log.warn "Lock validation: ${positions.size()} RentalAutomator codes found on ${lock} at positions ${positions}"
        sendNotification "Warning: Duplicate codes detected on ${lock}. Manual review recommended."
        recordAnalyticsEvent("lock_conflict_detected", "Lock: ${lock}, Positions: ${positions}")
        return false
    }
    return true
}

/**
 * Finds the next available code slot position on a lock.
 * Returns null if the lock is at maximum capacity.
 * @param lock The lock device to check
 * @return Next available position (integer), or null if lock is full
 */
def findNextAvailableCodePosition(lock) {
    def codes = lock.currentValue("lockCodes", true)
    def maxCodesValue = lock.currentValue("maxCodes")
    if(maxCodesValue == null) {
        log.warn "Code slot search: Lock ${lock} does not report maxCodes; defaulting to 30"
        maxCodesValue = 30
    }
    int maxCodes = maxCodesValue.toInteger()
    def codeJson = new JsonSlurper().parseText(codes)
    def usedPositions = codeJson.keySet().collect { it.toInteger() }
    for(int pos = 1; pos <= maxCodes; pos++) {
        if(!usedPositions.contains(pos)) {
            debugLog "Code slot search: Next available position: ${pos}"
            return pos
        }
    }
    log.warn "Code slot search: No available positions (max ${maxCodes} reached)"
    return null
}

/**
 * Programs a door lock code with the specified code and guest name.
 * Sanitizes guest name and masks code in debug logs for security.
 * Records pending operation for async verification.
 * @param lock The lock device to program
 * @param position The code slot position
 * @param code The door code to program
 * @param guestname The guest name to associate with the code
 * @return true if successful, false on error
 */
def programDoorLockCode(lock, position, code, guestname) {
    // Sanitize guest name to prevent injection and ensure safe lock display
    def safeName = sanitizeGuestName(guestname)
    // Security: Mask code in debug logs
    debugLog "Lock command: Programming ${lock} at position ${position}, code ${maskCode(code)}, name ${safeName}"
    try {
        // Record pending operation for async verification
        recordPendingOperation(lock.toString(), "program")
        lock.setCode(position, code, "RentalAutomator ${safeName}")
        log.info "Lock command: setCode sent to ${lock} at position ${position}"
    } catch(Exception e) {
        log.error "Lock command: Error programming code: ${e}"
        return false
    }
    return true
}

/**
 * Deletes a code from a door lock at the specified position.
 * Records pending operation for async verification.
 * @param lock The lock device
 * @param position The code slot position to delete
 * @return true if successful, false on error
 */
def deleteDoorLockCode(lock, position) {
    debugLog "Lock command: Deleting position ${position} from ${lock}"
    try {
        // Record pending operation for async verification
        recordPendingOperation(lock.toString(), "delete")
        lock.deleteCode(position.toInteger())
        log.info "Lock command: deleteCode sent to ${lock} at position ${position}"
    } catch(Exception e) {
        log.error "Lock command: Error deleting code: ${e}"
        return false
    }
    debugLog "Lock command: Deleted position ${position} from ${lock}"
    return true
}

//--------------------------------------------------------------
// Optional test function for door lock programming
//--------------------------------------------------------------

/**
 * Tests door lock programming by attempting to program a test code.
 * Uses a dummy code "1234" and guest name "TestGuest".
 * Note: The test code will remain on the lock until the next check-out or manual removal.
 */
def testDoorLockProgramming() {
    doorLocks.each { lock ->
        if(attemptProgramLock(lock, "1234", "Test")) {
            log.info "Lock test: Programming succeeded for ${lock}"
        } else {
            log.info "Lock test: Programming failed for ${lock}"
        }
    }
}