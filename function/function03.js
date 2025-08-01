const hccEventType = "NORMAL_DEDUCT_POINT";
const memberTypes = ["SOGO_VIP", "SOGO_VVIP"];
const appId = "A000111222";
const branchId = "BRANCH_JS";
const now = new Date();

db.events.aggregate([

  // 1️⃣ 初步篩選符合條件的活動（分館 + 時間 + 開放平台 + 類型）
  {
    $match: {
      eventStatus: "USING",                   // 活動啟用中
      onlyApp: true,                          // 限 App 參加
      hccEventType: hccEventType,             // 符合指定活動類型
      eventBranchId: branchId,                // 指定分館主辦活動
      startDate: { $lte: now },               // 已開始
      endDate: { $gte: now },                 // 尚未結束
      $or: [
        { hccEventType: { $ne: "CLUB_DEDUCT_POINT" } },     // 非 CLUB 活動
        { memberTypes: { $in: memberTypes } }               // CLUB 活動需檢查會員身分
      ]
    }
  },

  // 2️⃣ 加入 event_attendees：查找使用者是否在參與名單中
  {
    $lookup: {
      from: "event_attendees",
      let: { eventNo: "$_id" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$eventNo", "$$eventNo"] },
                { $eq: ["$appId", appId] }
              ]
            }
          }
        },
        { $limit: 1 }
      ],
      as: "matchedAttendee"
    }
  },

  // 3️⃣ 判斷活動是否開放會員或 app_id 是否在名單中
  {
    $match: {
      $or: [
        { allMember: true },
        { "matchedAttendee.0": { $exists: true } }
      ]
    }
  },

  // 4️⃣ 輸出格式處理：直接輸出陣列欄位，不做合併字串
  {
    $project: {
      _id: 0,
      eventNo: "$_id",              // 活動主鍵
      eventBranchId: 1,             // 活動所屬分館
      eventName: "$name",           // 活動名稱
      startDate: 1,
      endDate: 1,
      usingBranchIds: 1,            // 直接輸出陣列，不合併
      usingBranchNames: 1,
      giftInforUrl: 1,
      giftAttentionUrl: 1,
      eventMemo: 1,
      prizeCouponJson: 1
    }
  }

]).toArray()



