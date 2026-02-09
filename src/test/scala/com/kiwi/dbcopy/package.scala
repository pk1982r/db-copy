package com.kiwi

import com.kiwi.dbcopy.model.User

import java.time.Instant

package object dbcopy {

  extension (i: Int) {
    def userFromId: User = {
      User(s"$i", s"test$i@dot.com", Instant.now()) // TODO replace with CE TestControl when required
    }
  }

}
