const hccEventType = "NORMAL_DEDUCT_POINT";
const memberTypes = ["SOGO_VIP", "SOGO_VVIP"];
const appId = "A000111222";
const now = new Date();

db.events.aggregate([
  // 🎯 Step 1：挑出目前進行中、狀態啟用的活動
  {
    $match: {
      hccEventType: hccEventType,             // 活動類型
      eventStatus: "USING",                   // 活動狀態
      onlyApp: true,                          // 只限 App
      startDate: { $lte: now },               // 起始時間在現在之前
      endDate: { $gte: now },                 // 結束時間在現在之後
      $or: [
        { hccEventType: { $ne: "CLUB_DEDUCT_POINT" } },    // 非 club 類活動
        { memberTypes: { $in: memberTypes } }              // 或符合會員別
      ]
    }
  },

  // 🔗 Step 2：Join event_attendees（subquery 关联一位 appId）
  {
    $lookup: {
      from: "event_attendees",
      let: { eventNo: "$eventNo" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$eventNo", "$$eventNo"] },
                { $eq: ["$appId", appId] }              // 判斷參與者是否存在
              ]
            }
          }
        },
        { $limit: 1 }  // 最快取一筆即可
      ],
      as: "matchedAttendee"
    }
  },

  // ✅ Step 3：過濾：允許參加的情況
  {
    $match: {
      $or: [
        { allMember: true },                               // 開放所有人參加
        { "matchedAttendee.0": { $exists: true } }         // 或參加者有在名單中
      ]
    }
  },

  // ✅ Step 4：是否有符合條件的活動
  {
    $limit: 1
  },

  // 🔚 Step 5：轉換成 hasEvent: true 結果格式
  {
    $project: {
      _id: 0,
      hasEvent: { $literal: true }
    }
  }
]).toArray()
