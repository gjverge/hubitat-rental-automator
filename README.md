This is a fork of the https://github.com/TheDanHealy/hubitat-rental-automator lock automation code for Hubitat written in Groovy. Like the origonal author I was a user of Rboy lock apps on Smartthings until they droped support for Groovy apps without usable warning or a replacement. I switched to Hubitat and was looking for a solution when I found this project that did the core of what I needed but was missing some features that were required for my use case. The main item being that I use OwnerRez as my aggregation platform and needed to be able to parse its ical format. I added ical parsing details in the app configuration page. The new ical parsing makes it easy to support multiple ical formats and VRBO is an obvious addition but I don't have access to a VRBO system to properly configure and test its ical format. I also wanted to make operation as robust as possible as I often get short internet outages that may block ical fetches at the instant of lock change times. I started with just this minor ical change but have steadily been improving it and now it is ready to be shared with others. 

# What Changed vs. the original Rental Automator
This is a ground-up rewrite of Dan Healy's original `rental-automator.groovy`. The core concept is the same -- automatically program door lock codes and switch Hubitat modes based on an iCal calendar feed -- but nearly every subsystem has been reworked. Here is a detailed summary of the changes.

## OwnerRez Calendar Support
The original app only supported AirBNB iCal feeds. This version adds full OwnerRez support. A new **Calendar Format** dropdown lets you choose between AirBNB and OwnerRez. Each format has its own dedicated parser (`parseAirBNBCalendar` / `parseOwnerRezCalendar`) behind a common `parseCalendarData` dispatcher. OwnerRez events use `STATUS: CONFIRMED` (instead of `SUMMARY: Reserved`), `DoorCode` and `FirstName` custom fields, and a datetime format (`yyyyMMdd'T'HHmmss`) that carries exact check-in/check-out times. The restructuring makes it easy to add other custom ical parsers like for VRBO but as I don't have access to a VRBO ical version I did not add that support.

## Exact-Time Scheduling (OwnerRez)
When using OwnerRez with the **"Use OwnerRez times"** toggle enabled, the app schedules check-in and check-out at the exact times embedded in the booking rather than using a fixed daily time. A 15-minute polling loop (`pollIcalForUpdates`) watches for time changes (e.g., early check-in or late check-out granted by the host) and automatically reschedules. If the scheduled time has already passed when the app is enabled, it executes the procedure immediately.

## Check-In / Check-Out Time Offsets
Two new settings -- **checkinEarlyMinutes** (0-60) and **checkoutLateMinutes** (0-60) -- let you shift the scheduled procedures earlier or later without changing the actual check-in/check-out times. This was a Rboy app feature that I used heavily.

## Multi-Booking Support
The original code looked for a single check-in or check-out event per day. RentalLock uses `findAllCheckinEvents` and `findAllCheckoutEvents` to return lists, so multiple bookings starting or ending on the same day are each processed. Warnings and notifications are sent when overlapping bookings are detected.

## Retry Logic Overhaul
The original inline retry loop ran up to 10 times with a fixed 10-second pause. RentalLock replaces this with a generic `attemptWithRetry` wrapper used by both `attemptProgramLock` (3 retries, 10s delay) and `attemptDeleteLock` (3 retries, 10s delay). Each attempt is verified by reading back the lock codes, and retry statistics (average attempts, first-try percentage) are tracked in analytics.

## Safety Code Cleanup
A new `safetyCodeCleanup` procedure is scheduled 1 hour after every check-out as a failsafe. It verifies no lingering RentalAutomator codes remain on any lock. The check-out procedure itself also does an immediate post-deletion sweep and removes any duplicate codes it finds.

## Lock Event Subscription
The app now subscribes to each lock's `codeChanged` events via `lockCodeChangedHandler`. When a lock reports a code set/deleted/failed event, the handler cross-references it against a `pendingLockOperations` map to confirm or flag the operation asynchronously.

## Security Hardening
- **HTTPS enforcement**: Calendar URLs must use HTTPS; HTTP is rejected.
- **Guest name sanitization**: `sanitizeGuestName` strips non-alphanumeric characters and caps length at 20 characters.
- **Door code validation**: `isValidDoorCode` requires 4-8 numeric digits.
- **Code masking**: Debug logs show only the last 2 digits of door codes (`maskCode`).
- **URL / PII protection**: The raw calendar URL and iCal data are no longer logged.

## Calendar Fetch Resilience
- **Caching**: The last successful calendar parse is stored in `state.iCalDictPrevious`. If a fetch fails, the cached data is used as a fallback and a notification is sent.
- **Rate limiting**: A 5-minute minimum interval (`CALENDAR_FETCH_MIN_INTERVAL`) prevents excessive API calls. Debug mode and manual tests bypass the limit.

## Analytics Dashboard
A new **Analytics** section in the UI displays an HTML table of success/failure counts and rates for check-ins, check-outs, lock programming, and lock code deletion. It also shows retry statistics (average attempts, first-try percentage), timestamps of the last check-in/check-out, and a toggleable list of the 20 most recent events.

## Configuration Validation
`validateConfiguration` runs on every save and checks for: HTTPS on the calendar URL, required modes being set, valid prep minutes, lock capabilities (`setCode` / `deleteCode`), and valid time-offset ranges. Errors are logged and stored in `state.configurationValid`.

## State Migration
A versioned migration system (`migrateState`) runs on install and update. It inspects `state.appVersion` and applies incremental migrations (currently v1 -> v2: adds `recentEvents`, `pendingLockOperations`, `configurationValid`). Future upgrades can add new migration blocks without breaking existing installs.

## Dynamic Page UI
The preferences page was converted from a static `page()` declaration to a `dynamicPage` method (`mainPage()`). This allows button labels to update (e.g., a single toggle button instead of separate enable/disable buttons), conditional sections to render properly, and analytics to refresh on each page load.

## Shared Check-In Logic
The original code duplicated nearly identical logic in `checkinProcedure` and `checkinPrepProcedure`. RentalLock extracts the common flow into `processCheckinEvent(mode, forceOverride, programLock)` which both procedures call with different arguments.

## Lock Code Naming
Codes are now named `"RentalAutomator {GuestName}"` instead of just `"RentalAutomator"`, making it easier to identify which guest a code belongs to in the lock's code list. If the Guest Name is not availble it is left blank.

## Improved Code Slot Search
`findNextAvailableCodePosition` now iterates positions 1 through `maxCodes` looking for the first unused slot (gap-aware), rather than assuming codes are contiguous. It also handles locks that don't report `maxCodes` by defaulting to 30.

## atomicState for Thread Safety
`state.enabled` was renamed to `atomicState.automationEnabled`, and the calendar-test flags moved to `atomicState`, to prevent race conditions in Hubitat's concurrent execution model.

## Misc Cleanup
- `debugLog` helper replaces repetitive `if(debugMode) log.debug` guards throughout.
- `convertTimeToCron` gained a `minutesToAdd` parameter; a new `convertDateToCron` converts Date objects directly.
- `extractProperty` regex broadened from `"propertyName:(value)\r?\n"` to `"propertyName...:\h*(word)"` for more flexible iCal parsing.
- Dead functions (`checkinToday`, `checkoutToday`, `iCalToMapListAirBnB`) removed.

---

# Hubitat Rental Automator by [Dan Healy](https://thedanhealy.com)

This Hubitat app provides a direct integration with AirBNB and control over changing modes and programming door locks **without the use of any other services, such as the Maker API and/or other applications**. 

I wrote this app for a non-profit organization which I serve on the Board of Directors as the Vice President. We operate a 49 acre property in Ohio, called the Minton Lodge, that we offer to families who have gone through cancer or military journeys for completely free as a therapeutic and relaxation retreat. I originally outfitted the Minton Lodge with SmartThings because the mobile app interface was easier for my fellow Board members and volunteers to learn, and we used the famous Rental Lock Automator from RBoy. But, since Jan 2022 when SmartThings stopped supporting the Groovy-based apps platform and RBoy didn't provide any path forward, we started inputting door lock codes manually and carefully arming/disarming the lodge.

If you enjoy using this app, please consider donating to the **[Josh Minton Foundation](https://brotherson3.org)**. I created this app to help our foundation better manage the AirBNB guests, which provides us with extra funding in-between our no-cost therapeutic stays. I am now offering this app to the community for free, but in hopes that you will also donate back in appreciation for the free usage.

**This app only works with the AirBNB calendar at the present time. I am committed to continuing to update this app with new functionality and review all pull requests submitted.**

# Prerequisites

The following items are required for this app to operate with your Hubitat:

- Programmable Door Lock
- AirBNB Calendar URL (Learn more [here](https://www.airbnb.com/help/article/99#section-heading-9-0))
- "Mode" in Hubitat that you want to use for Check-In, Check-In Prep (that's a certain amount of time before check-in when you want to start doing something, like cooling or heating), and Check-Out. You can learn about how to do this [here](https://docs2.hubitat.com/how-to/add-or-change-a-mode#:~:text=In%20order%20to%20start%20using,in%20the%20list%20of%20settings).

# Setup Instructions

## Easy Installation using Hubitat Package Manager

1. Install Hubitat Package Manager (per instructions [here](https://hubitatpackagemanager.hubitatcommunity.com/installing.html))
1. In the Hubitat menu, go to "Apps" -> "Hubitat Package Manager" -> "Install" -> "Search By Keyword"
1. In the search criteria field, type "Renatal Automator" and click "Next". You can leave "Fast Search" on or off.
1. Click on "Rental Automator by TheDanHealy"
1. On the "Install a Package from a Repository" page, click "Next"
1. On the "Ready to install" page, leave the toggle switch on to configure the installed package after the installation completes and click "Next"
1. On the "Installation complete" page, click "Next"
1. Start filling in the settings for the Rental Automator app and enjoy

## Manual Installation

This method does not inform you when there are updates available.

1. Log into your Hubitat, go to "Apps Code" and click the button for "+ New App"
1. Click the "Import" button, paste the URL https://raw.githubusercontent.com/TheDanHealy/hubitat-rental-automator/main/rental-automator/rental-automator.groovy, click the button "Import", click "OK to confirm you want to overwrite, and finally click the "Save" button.
1. In the Hubitat menu, go to "Apps", click the button "+ Add User App", and finally click on "Hubitat Rental Automator"
1. Supply all the required information in the settings page and click the button "Save"
1. Click the button "Test" to test and verify there's no issues with your AirBNB Calendar URL
1. If everything looks OK, finally, click the button "Enable AirBNB Automation"

At this point, you can use whatever app you please to control what automations happen when the configured "Modes" (like Check-In) are triggered. I like to the the Simple Automation Rules or Rule Machine to turn on/off lights, set thermostats, and more.

That's it :)

# Having any issue?

If you're having any issues with the app, please first enable Debug mode, view the [logs](https://docs2.hubitat.com/how-to/collect-information-for-support#:~:text=Open%20your%20logs%20by%20selecting,depending%20on%20your%20log%20settings), and repeat the issue. Review the logs

If you want / need to submit any issue, please click on the tab above "Issues" and create a new GitHub Issue.

# Message for developers

I encourage you to please help build and improve this app. Fork this repo, add enhancements, fixes, and improvements, then submit a PR back to here.
